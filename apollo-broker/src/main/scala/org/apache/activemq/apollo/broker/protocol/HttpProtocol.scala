/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.broker.protocol
import org.fusesource.hawtdispatch.transport.{TcpTransport, AbstractProtocolCodec, SslProtocolCodec}
import org.fusesource.hawtbuf.{AsciiBuffer, Buffer}
import org.apache.activemq.apollo.broker.Connector
import org.apache.activemq.apollo.dto.SslDTO
import org.fusesource.hawtdispatch.transport.SslProtocolCodec.ClientAuth
import org.fusesource.hawtdispatch.transport.AbstractProtocolCodec.Action
import org.fusesource.hawtdispatch._

/**
 */
object HttpProtocol extends Protocol {
  def id(): String = "http"

  val http_methods = Set(
    "HEAD", "GET", "POST", "PUT",
    "DELETE", "TRACE", "OPTIONS",
    "PATCH", "CONNECT").map(x=>new AsciiBuffer(x+" "))



  override def isIdentifiable = true
  override val maxIdentificaionLength = http_methods.foldLeft(0){ case (rc, x)=> rc.max(x.length) }

  override def matchesIdentification(buffer: Buffer):Boolean = {
    for( method <- http_methods ) {
      if( buffer.startsWith(method) ) {
        return true
      }
    }
    false
  }

  class HttpCodec extends AbstractProtocolCodec {
    protected def encode(value: Any) = sys.error("Should not be called.")
    protected def initialDecodeAction(): Action = new Action() {
      def apply() = null
    }
    def read_buffer = readBuffer
  }
  def createProtocolCodec(connector:Connector) = new HttpCodec



  def createProtocolHandler = new ProtocolHandler {
    def protocol = id
    def session_id = None
    override def on_transport_connected = {
      connection.transport match {
        case transport:TcpTransport =>
          transport.getProtocolCodec() match {
            case codec:HttpCodec =>

              // Lets take the channel away from the transport,
              // close it out..
              val channel = transport.getSocketChannel
              transport.setCloseOnCancel(false)
              val putback = codec.read_buffer
              putback.position(0)

              // Then pass the channel to the web server.
              connection.connector.broker.web_server.accept(channel, putback)
          }

      }
      connection.stop(NOOP)
    }
  }
}
