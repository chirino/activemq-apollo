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
package org.apache.activemq.apollo.broker.store.bdb

import dto.{BDBStoreDTO, BDBStoreStatusDTO}
import collection.Seq
import org.fusesource.hawtdispatch._
import java.util.concurrent._
import atomic.{AtomicReference, AtomicLong}
import org.apache.activemq.apollo.broker.store._
import org.apache.activemq.apollo.util._
import org.fusesource.hawtdispatch.ListEventAggregator
import org.apache.activemq.apollo.dto.StoreStatusDTO
import org.apache.activemq.apollo.util.OptionSupport._
import java.io.{InputStream, OutputStream}
import org.fusesource.hawtbuf.Buffer

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object BDBStore extends Log {
  val DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class BDBStore(var config:BDBStoreDTO) extends DelayingStoreSupport {


  var next_queue_key = new AtomicLong(1)
  var next_msg_key = new AtomicLong(1)

  var write_executor:ExecutorService = _
  var read_executor:ExecutorService = _
  val client = new BDBClient(this)

  override def toString = "bdb store at "+config.directory

  def flush_delay = config.flush_delay.getOrElse(500)
  
  protected def get_next_msg_key = next_msg_key.getAndIncrement

  protected def store(uows: Seq[DelayableUOW])(callback: =>Unit) = {
    write_executor {
      client.store(uows, ^{
        dispatch_queue {
          callback
        }
      })
    }
  }

  protected def _start(on_completed: Task) = {
    write_executor = Executors.newFixedThreadPool(1, new ThreadFactory(){
      def newThread(r: Runnable) = {
        val rc = new Thread(r, "bdb store io write")
        rc.setDaemon(true)
        rc
      }
    })
    read_executor = Executors.newFixedThreadPool(config.read_threads.getOrElse(10), new ThreadFactory(){
      def newThread(r: Runnable) = {
        val rc = new Thread(r, "bdb store io read")
        rc.setDaemon(true)
        rc
      }
    })
    client.config = config
    poll_stats
    write_executor {
      client.start()
      next_msg_key.set( client.getLastMessageKey +1 )
      next_queue_key.set( client.getLastQueueKey +1 )
      on_completed.run
    }
  }

  protected def _stop(on_completed: Task) = {
    new Thread() {
      override def run = {
        write_executor.shutdown
        write_executor.awaitTermination(60, TimeUnit.SECONDS)
        write_executor = null
        read_executor.shutdown
        read_executor.awaitTermination(60, TimeUnit.SECONDS)
        read_executor = null
        client.stop
        on_completed.run
      }
    }.start
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the Store interface
  //
  /////////////////////////////////////////////////////////////////////

  /**
   * Deletes all stored data from the store.
   */
  def purge(callback: =>Unit) = {
    write_executor {
      client.purge()
      next_queue_key.set(1)
      next_msg_key.set(1)
      callback
    }
  }


  def get_map_entry(key: Buffer)(callback: (Option[Buffer]) => Unit) = {
    read_executor {
      callback(client.get(key))
    }
  }

  def get_prefixed_map_entries(prefix:Buffer)(callback: Seq[(Buffer, Buffer)]=>Unit) = {
    read_executor {
      callback(client.get_prefixed_map_entries(prefix))
    }
  }


  /**
   * Ges the last queue key identifier stored.
   */
  def get_last_queue_key(callback:(Option[Long])=>Unit):Unit = {
    write_executor {
      callback(Some(client.getLastQueueKey))
    }
  }

  def add_queue(record: QueueRecord)(callback: (Boolean) => Unit) = {
    write_executor {
     client.addQueue(record, ^{ callback(true) })
    }
  }

  def remove_queue(queueKey: Long)(callback: (Boolean) => Unit) = {
    write_executor {
      client.removeQueue(queueKey,^{ callback(true) })
    }
  }

  def get_queue(queueKey: Long)(callback: (Option[QueueRecord]) => Unit) = {
    write_executor {
      callback( client.getQueue(queueKey) )
    }
  }

  def list_queues(callback: (Seq[Long]) => Unit) = {
    write_executor {
      callback( client.listQueues )
    }
  }

  val load_source = createSource(new ListEventAggregator[(Long, (Option[MessageRecord])=>Unit)](), dispatch_queue)
  load_source.setEventHandler(^{drain_loads});
  load_source.resume


  def load_message(messageKey: Long, locator:AtomicReference[Object])(callback: (Option[MessageRecord]) => Unit) = {
    message_load_latency_counter.start { end=>
      load_source.merge((messageKey, { (result)=>
        end()
        callback(result)
      }))
    }
  }

  def drain_loads = {
    var data = load_source.getData
    message_load_batch_size_counter += data.size
    read_executor ^{
      client.loadMessages(data)
    }
  }

  def list_queue_entry_ranges(queueKey: Long, limit: Int)(callback: (Seq[QueueEntryRange]) => Unit) = {
    write_executor ^{
      callback( client.listQueueEntryGroups(queueKey, limit) )
    }
  }

  def list_queue_entries(queueKey: Long, firstSeq: Long, lastSeq: Long)(callback: (Seq[QueueEntryRecord]) => Unit) = {
    write_executor ^{
      callback( client.getQueueEntries(queueKey, firstSeq, lastSeq) )
    }
  }

  def poll_stats:Unit = {
    def displayStats = {
      if( service_state.is_started ) {

        flush_latency = flush_latency_counter(true)
        message_load_latency = message_load_latency_counter(true)
//        client.metric_journal_append = client.metric_journal_append_counter(true)
//        client.metric_index_update = client.metric_index_update_counter(true)
        close_latency = close_latency_counter(true)
        message_load_batch_size =  message_load_batch_size_counter(true)

        poll_stats
      }
    }

    dispatch_queue.executeAfter(1, TimeUnit.SECONDS, ^{ displayStats })
  }

  def get_store_status(callback:(StoreStatusDTO)=>Unit) = dispatch_queue {
    val rc = new BDBStoreStatusDTO
    fill_store_status(rc)
    rc.message_load_batch_size = message_load_batch_size
    callback(rc)
  }

  /**
   * Exports the contents of the store to the provided streams.  Each stream should contain
   * a list of framed protobuf objects with the corresponding object types.
   */
  def export_data(os:OutputStream, cb:(Option[String])=>Unit) = write_executor {
    cb(client.export_data(os))
  }

  /**
   * Imports a previously exported set of streams.  This deletes any previous data
   * in the store.
   */
  def import_data(is:InputStream, cb:(Option[String])=>Unit) = write_executor {
    cb(client.import_data(is))
  }
}
