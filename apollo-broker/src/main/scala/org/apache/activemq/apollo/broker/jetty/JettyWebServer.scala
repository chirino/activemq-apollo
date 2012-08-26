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
package org.apache.activemq.apollo.broker.jetty

import org.eclipse.jetty.server.{Connector, Handler, Server}
import org.apache.activemq.apollo.broker.Broker
import org.eclipse.jetty.webapp.WebAppContext
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.apache.activemq.apollo.util._
import org.fusesource.hawtdispatch._
import org.eclipse.jetty.server.handler.HandlerList
import collection.mutable.HashMap
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector
import javax.net.ssl.SSLContext
import org.eclipse.jetty.util.thread.ExecutorThreadPool
import org.apache.activemq.apollo.dto.WebAdminDTO
import java.net.{URL, URI}
import java.io.{FileOutputStream, File}
import java.util.jar.JarInputStream
import java.lang.String
import org.eclipse.jetty.servlet.{FilterMapping, FilterHolder}
import org.apache.activemq.apollo.broker.web.{AllowAnyOriginFilter, WebServer, WebServerFactory}
import javax.servlet._
import org.fusesource.hawtdispatch.transport.{TcpTransport, Transport}
import org.fusesource.hawtdispatch._
import java.nio.channels.{SelectionKey, SocketChannel}
import java.nio.ByteBuffer
import org.eclipse.jetty.io.nio.{SelectorManager, SelectChannelEndPoint}
import java.util.concurrent.ConcurrentHashMap
import java.lang.reflect.Modifier

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object JettyWebServerFactory extends WebServerFactory {

  // Enabled this factory if we can load the jetty classes.
  val enabled = try {
    this.getClass.getClassLoader.loadClass(classOf[WebAppContext].getName)
    true
  } catch {
    case _ =>
    false
  }

  def create(broker:Broker): WebServer = {
    if( !enabled ) {
      return null
    }
    new JettyWebServer(broker)
  }

}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object JettyWebServer extends Log {


  def webapp(tmp:File) = {
    import FileSupport._

    var rc:File = null
    val loader = JettyWebServer.getClass.getClassLoader

    if( System.getProperty("apollo.webapp")!=null ) {
      rc = new File(System.getProperty("apollo.webapp"))
    } else {
      // Unpack all the webapp resources found on the classpath.
      val resources = loader.getResources("META-INF/services/org.apache.activemq.apollo/webapp-resources.jar")
      while( resources.hasMoreElements ) {
        val url = resources.nextElement();
        import FileSupport._
        rc = tmp / "webapp-resources"
        rc.mkdirs()
        using(new JarInputStream(url.openStream()) ) { is =>
          var entry = is.getNextJarEntry
          while( entry!=null ) {
            if( entry.isDirectory ) {
              (rc / entry.getName).mkdirs()
            } else {
              using(new FileOutputStream( rc / entry.getName )) { os =>
                copy(is, os)
              }
            }
            is.closeEntry()
            entry = is.getNextJarEntry
          }
        }
      }
    }

    // the war might be on the classpath..
    if( rc==null ) {
      val bootClazz: String = "org/apache/activemq/apollo/web/Boot.class"
      var url = loader.getResource(bootClazz)
      rc = if(url==null) {
        null
      } else {
        if( url.getProtocol == "file") {
          // we are probably being run from an IDE...
          val classes_dir = new File( url.getFile.stripSuffix("/"+bootClazz) )
          if( classes_dir.isDirectory ) {
            classes_dir/".."/".."/"src"/"main"/"webapp"
          } else {
            null
          }
        } else {
          null
        }

      }
    }
    rc
  }

  // Let get turn a 'private final' field, into a 'public non-final' :)
  // so that we can 'put back' they bytes we read to detect the
  // protocol.
  val ChannelEndPoint_channel_field = {
    try {
      val field = classOf[org.eclipse.jetty.io.nio.ChannelEndPoint].getDeclaredField("_channel")
      field.setAccessible(true)
      val modifiersField = classOf[java.lang.reflect.Field].getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
      field
    } catch {
      case e =>
        e.printStackTrace()
        null
    }
  }

}

class JettyWebServer(val broker:Broker) extends WebServer with BaseService {
  import JettyWebServer._

  var server:Server = _

  override def toString: String = "jetty webserver"

  val dispatch_queue = createQueue()
  var web_admins = List[WebAdminDTO]()
  var uri_addresses = List[URI]()

  object apollo_connector extends SelectChannelConnector {

    override def doStart() {
      _buffers.start()
      getSelectorManager.setSelectSets(getAcceptors());
      getSelectorManager.setMaxIdleTime(getMaxIdleTime());
      getSelectorManager.setLowResourcesConnections(getLowResourcesConnections());
      getSelectorManager.setLowResourcesMaxIdleTime(getLowResourcesMaxIdleTime());
      getSelectorManager.start()
    }
    override def doStop() {
      getSelectorManager.stop()
      _buffers.stop()
    }
    override def getLocalPort = 0
    override def getPort = 0


    val putback_map = new ConcurrentHashMap[SocketChannel, ByteBuffer]()

    def accept(channel: SocketChannel, putback:ByteBuffer) = {
      if( putback!=null && putback.hasRemaining ) {
        putback_map.put(channel, putback)
      }

      val server = getServer
      val manager = getSelectorManager
      if (server!=null && server.isStarted && manager.isStarted) {
        channel.configureBlocking(false);
        configure(channel.socket());
        manager.register(channel);
      }
    }

    override def newEndPoint(channel: SocketChannel, selectSet: SelectorManager#SelectSet, key: SelectionKey): SelectChannelEndPoint = {
      val rc= new SelectChannelEndPoint(channel,selectSet,key, getMaxIdleTime);
      val putback = putback_map.remove(channel)
      if ( putback!=null ) {
        // dirty.. dirty.. yeah I know!
        ChannelEndPoint_channel_field.set(rc, new PutBackByteChannel(channel, putback));
      }
      rc.setConnection(selectSet.getManager().newConnection(channel,rc, key.attachment()));
      return rc;
    }

  }

  def accept(channel: SocketChannel, putback:ByteBuffer) = Broker.BLOCKABLE_THREAD_POOL {
    apollo_connector.accept(channel, putback)
  }

  protected def _start(on_completed: Task) = Broker.BLOCKABLE_THREAD_POOL {
    this.synchronized {
      import OptionSupport._
      import FileSupport._
      import collection.JavaConversions._

      val config = broker.config

      val webapp_path = webapp(broker.tmp)
      if(webapp_path==null ) {
        warn("Administration interface cannot be started: webapp resources not found");
      } else {
        // Start up the admin interface...
        debug("Starting administration interface");

        if( broker.tmp !=null ) {
          System.setProperty("scalate.workdir", (broker.tmp / "scalate").getCanonicalPath )
        }

        val contexts = HashMap[String, Handler]()
        val connectors = HashMap[String, Connector]()
        connectors.put("", apollo_connector)

        web_admins = config.web_admins.toList
        web_admins.foreach { web_admin =>

          val bind = web_admin.bind.getOrElse("http://127.0.0.1:61680")
          val bind_uri = new URI(bind)
          val prefix = "/"+bind_uri.getPath.stripPrefix("/")

          val scheme = bind_uri.getScheme
          val host = bind_uri.getHost
          var port = bind_uri.getPort

          var query = URISupport.parseQuery(bind_uri.getQuery)
          val cors_origin = query.get("cors_origin")

          scheme match {
            case "http" =>
              if (port == -1) {
                port = 80
              }
            case "https" =>
              if (port == -1) {
                port = 443
              }
            case _ => throw new Exception("Invalid 'web_admin' bind setting.  The protocol scheme must be http or https.")
          }

          // Only add the connector if not yet added..
          val connector_id = scheme+"://"+host+":"+port
          if ( !connectors.containsKey(connector_id) ) {


            val connector = scheme match {
              case "http" => new SelectChannelConnector
              case "https" =>
                val sslContext = if( broker.key_storage!=null ) {
                  val protocol = "TLS"
                  val sslContext = SSLContext.getInstance (protocol)
                  sslContext.init(broker.key_storage.create_key_managers, broker.key_storage.create_trust_managers, null)
                  sslContext
                } else {
                  SSLContext.getDefault
                }

                val connector = new SslSelectChannelConnector
                connector.setSslContext(sslContext)
                connector.setWantClientAuth(true)
                connector
            }

            connector.setHost(host)
            connector.setPort(port)
            connectors.put(connector_id, connector)
          }

          // Only add the app context if not yet added..
          if ( !contexts.containsKey(prefix) ) {
            var context = new WebAppContext
            context.setContextPath(prefix)
            context.setWar(webapp_path.getCanonicalPath)
            context.setClassLoader(Broker.class_loader)

            if( cors_origin!=null && !cors_origin.trim().isEmpty ) {
              val origins = cors_origin.split(",").map(_.trim()).toSet
              context.addFilter(new FilterHolder(new AllowAnyOriginFilter(origins)), "/*", FilterMapping.DEFAULT)
            }
            context.addFilter(new FilterHolder(new Filter(){
              def init(p1: FilterConfig) {}
              def destroy() {}
              def doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) = {
                request.setAttribute("APOLLO_BROKER", broker)
                chain.doFilter(request, response)
              }
            }), "/*", FilterMapping.DEFAULT)

            if( broker.tmp !=null ) {
              context.setTempDirectory(broker.tmp)
            }
            contexts.put(prefix, context)
          }
        }


        val context_list = new HandlerList
        contexts.values.foreach(context_list.addHandler(_))

        server = new Server
        server.setHandler(context_list)
        server.setConnectors(connectors.values.toArray)
        server.setThreadPool(new ExecutorThreadPool(Broker.BLOCKABLE_THREAD_POOL))
        server.start

        for( connector <- connectors.values ; prefix <- contexts.keys ) {
          val localPort = connector.getLocalPort
          val scheme = connector match {
            case x:SslSelectChannelConnector => "https"
            case _ => "http"
          }

          val uri:URI = new URI(scheme, null, connector.getHost, localPort, prefix, null, null)
          broker.console_log.info("Administration interface available at: %s", uri)
          uri_addresses ::= uri
        }

      }
      on_completed.run

    }
  }

  protected def _stop(on_completed: Task) = Broker.BLOCKABLE_THREAD_POOL {
    this.synchronized {
      if( server!=null ) {
        server.stop
        server = null
        uri_addresses = Nil
      }
      on_completed.run
    }
  }

  def uris() = uri_addresses.toArray

  def update(on_complete: Task) = dispatch_queue {
    import collection.JavaConversions._
    val new_list = broker.config.web_admins.toList
    if( new_list != web_admins ) {
      // restart to pickup the changes.
      stop(^{
        start(on_complete)
      })
    } else {
      on_complete.run()
    }
  }
}