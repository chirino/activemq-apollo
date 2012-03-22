/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.broker;

import _root_.scala.collection.JavaConversions._
import org.fusesource.hawtdispatch._

import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.util.OptionSupport._
import org.apache.activemq.apollo.dto._
import security._
import security.SecuredResource.VirtualHostKind
import store._
import java.lang.{Throwable, String}
import java.util.concurrent.ConcurrentHashMap

trait VirtualHostFactory {
  def create(broker:Broker, dto:VirtualHostDTO):VirtualHost
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object VirtualHostFactory {

  val finder = new ClassFinder[VirtualHostFactory]("META-INF/services/org.apache.activemq.apollo/virtual-host-factory.index",classOf[VirtualHostFactory])

  def create(broker:Broker, dto:VirtualHostDTO):VirtualHost = {
    if( dto == null ) {
      return null
    }
    finder.singletons.foreach { provider=>
      val connector = provider.create(broker, dto)
      if( connector!=null ) {
        return connector;
      }
    }
    return null
  }
}

object DefaultVirtualHostFactory extends VirtualHostFactory with Log {

  def create(broker: Broker, dto: VirtualHostDTO): VirtualHost = dto match {
    case dto:VirtualHostDTO =>
      if( dto.getClass != classOf[VirtualHostDTO] ) {
        // ignore sub classes of AcceptingVirtualHostDTO
        null;
      } else {
        val rc = new VirtualHost(broker, dto.id)
        rc.config = dto
        rc
      }
    case _ =>
      null
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object VirtualHost extends Log {
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class VirtualHost(val broker: Broker, val id:String) extends BaseService with SecuredResource {
  import VirtualHost._
  
  override val dispatch_queue:DispatchQueue = createQueue("virtual-host")

  var config:VirtualHostDTO = _
  val router:Router = new LocalRouter(this)

  var names:List[String] = Nil;

  var store:Store = null
  val queue_id_counter = new LongCounter()

  val session_counter = new PersistentLongCounter("session_counter")

  var dead_topic_metrics = new DestMetricsDTO
  var dead_queue_metrics = new DestMetricsDTO
  var dead_dsub_metrics = new DestMetricsDTO

  var authenticator:Authenticator = _
  var authorizer = Authorizer()

  var audit_log:Log = _
  var security_log:Log  = _
  var connection_log:Log = _
  var console_log:Log = _

  var direct_buffer_allocator:DirectBufferAllocator = null

  private val _plugin_state = new ConcurrentHashMap[Class[_],  Any]()
  
  /** 
   * Plugins can associate state data with the virtual host instance
   * using this method.  The factory will be used to create the state
   * if it does not yet exist.
   */
  def plugin_state[T](factory: =>T, clazz:Class[T]):T = {
    var state = _plugin_state.get(clazz).asInstanceOf[T]
    if( state == null ) {
      state = factory
      if( state != null ) {
        _plugin_state.put(clazz, state)
      }
    }
    state
  }

  /**
   * Used to clear out previously set plugin state.  
   */
  def clear_plugin_state[T](clazz:Class[T]):T = {
    _plugin_state.remove(clazz).asInstanceOf[T]
  }  
  
  def resource_kind = VirtualHostKind

  @volatile
  var client_redirect:Option[String] = None

  override def toString = if (config==null) "virtual-host" else "virtual-host: "+config.id

  /**
   * Validates and then applies the configuration.
   */
  def update(config: VirtualHostDTO, on_completed:Task) = dispatch_queue {
    if ( !service_state.is_started ) {
      this.config = config
      on_completed.run
    } else {

      // in some cases we have to restart the virtual host..
      if( config.store != this.config.store ) {
        stop(^{
          this.config = config
          start(on_completed)
        })
      } else {
        this.config = config
        apply_update
        this.router.apply_update(on_completed)
      }
    }
  }

  def apply_update:Unit = {
    // Configure the logging categories...
    val log_category = config.log_category.getOrElse(new LogCategoryDTO)
    security_log = Option(log_category.security).map(Log(_)).getOrElse(broker.security_log)
    audit_log = Option(log_category.audit).map(Log(_)).getOrElse(broker.audit_log)
    connection_log = Option(log_category.connection).map(Log(_)).getOrElse(broker.connection_log)
    console_log = Option(log_category.console).map(Log(_)).getOrElse(broker.console_log)

    if (config.authentication != null) {
      if (config.authentication.enabled.getOrElse(true)) {
        // Virtual host has it's own settings.
        authenticator = new JaasAuthenticator(config.authentication, security_log)
      } else {
        // Don't use security on this host.
        authenticator = null
      }
    } else {
      // use the broker's settings..
      authenticator = broker.authenticator
    }
    if( authenticator!=null ) {
      val rules = config.access_rules.toList ::: broker.config.access_rules.toList
      authorizer = Authorizer(broker, this)
    } else {
      authorizer = Authorizer()
    }
  }

  override protected def _start(on_completed:Task):Unit = {
    apply_update

    if ( Option(config.heap_bypass).map(MemoryPropertyEditor.parse(_).toInt).getOrElse(0) > 0 ) {
      import org.apache.activemq.apollo.util.FileSupport._
      val tmp_dir = broker.tmp / "heapbypass" / id
      tmp_dir.recursive_delete
      direct_buffer_allocator = new ConcurrentFileDirectBufferAllocator(tmp_dir)
    }

    store = StoreFactory.create(config.store)

    val tracker = new LoggingTracker("virtual host startup", console_log)
    if( store!=null ) {
      val task = tracker.task("store startup")
      console_log.info("Starting store: "+store)
      store.start {
        {
          val task = tracker.task("store get last queue key")
          store.get_last_queue_key{ key=>
            key match {
              case Some(x)=>
                queue_id_counter.set(key.get)
              case None =>
                warn("Could not get last queue key")
            }
            task.run
          }

          if( config.purge_on_startup.getOrElse(false) ) {
            val task = tracker.task("store purge")
            store.purge {
              task.run
            }
          }
        }
        task.run
      }
    }

    tracker.callback {

      val tracker = new LoggingTracker("virtual host startup", console_log)

      // The default host handles persisting the connection id counter.
      if(store!=null) {
        if(session_counter.get == 0) {
          val task = tracker.task("load session counter")
          session_counter.init(store) {
            task.run()
          }
        } else {
          session_counter.connect(store)
        }
      }

      tracker.start(router)
      tracker.callback(on_completed)
    }

  }


  override protected def _stop(on_completed:Task):Unit = {

    val tracker = new LoggingTracker("virtual host shutdown", console_log)
    tracker.stop(router);
    if( store!=null ) {
      val task = tracker.task("store session counter")
      session_counter.disconnect{
        tracker.stop(store);
        task.run()
      }
    }
    tracker.callback(dispatch_queue.runnable {
      if( direct_buffer_allocator !=null ) {
        direct_buffer_allocator.close
        direct_buffer_allocator
      }
      on_completed.run()
    })
  }

  def local_router = router.asInstanceOf[LocalRouter]

  def reset_metrics = {
    dead_queue_metrics = new DestMetricsDTO
    dead_topic_metrics = new DestMetricsDTO
  }
  
  def aggregate_dest_metrics(metrics:Iterable[DestMetricsDTO]):AggregateDestMetricsDTO = {
    metrics.foldLeft(new AggregateDestMetricsDTO) { (to, from) =>
      DestinationMetricsSupport.add_destination_metrics(to, from)
      from match {
        case from:AggregateDestMetricsDTO =>
          to.objects += from.objects
        case _ =>
          to.objects += 1
      }
      to
    }
  }

  def get_topic_metrics:FutureResult[AggregateDestMetricsDTO] = {
    val topics:Iterable[Topic] = local_router.local_topic_domain.destinations
    val metrics: Future[Iterable[Result[DestMetricsDTO, Throwable]]] = Future.all {
      topics.map(_.status.map(_.map_success(_.metrics)))
    }
    metrics.map( x => Success {
      val rc = aggregate_dest_metrics(x.flatMap(_.success_option))
      DestinationMetricsSupport.add_destination_metrics(rc, dead_topic_metrics)
      rc
    })
  }
  
  def get_queue_metrics:FutureResult[AggregateDestMetricsDTO] = {
    val queues:Iterable[Queue] = local_router.local_queue_domain.destinations
    val metrics = sync_all (queues) { queue =>
      queue.get_queue_metrics
    }
    metrics.map( x => Success {
      val rc = aggregate_dest_metrics(x.flatMap(_.success_option))
      DestinationMetricsSupport.add_destination_metrics(rc, dead_queue_metrics)
      rc
    })
  }
  
  def get_dsub_metrics:FutureResult[AggregateDestMetricsDTO] = sync(this) {
    val dsubs:Iterable[Queue] = local_router.local_dsub_domain.destination_by_id.values
    val metrics = sync_all (dsubs) { dsub =>
      dsub.get_queue_metrics
    }
    metrics.map( x => Success {
      val rc = aggregate_dest_metrics(x.flatMap(_.success_option))
      DestinationMetricsSupport.add_destination_metrics(rc, dead_dsub_metrics)
      rc
    })
  }

}
