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
package org.apache.activemq.apollo.dto;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * <p>
 * Holds the address and port to bind the web interface
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class WebAdminDTO {

    /**
     * The address and port to bind the web interface on in URL syntax.
     */
    @XmlAttribute
    public String bind;

    /**
     * The list external domains that are allowed to
     * do Cross-Origin Resource Sharing with this broker.  Use
     * '*' to match all domains.
     */
    @XmlElement(name="cors_origin")
    public List<StringIdLabeledDTO> cors_origin = new ArrayList<StringIdLabeledDTO>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WebAdminDTO)) return false;

        WebAdminDTO that = (WebAdminDTO) o;

        if (bind != null ? !bind.equals(that.bind) : that.bind != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return bind != null ? bind.hashCode() : 0;
    }
}
