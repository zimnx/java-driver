/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.UUID;

public class SniEndPointFactory implements EndPointFactory {

  private final InetSocketAddress proxyAddress;

  public SniEndPointFactory(InetSocketAddress proxyAddress) {
    this.proxyAddress = proxyAddress;
  }

  @Override
  public void init(Cluster cluster) {}

  @Override
  public EndPoint create(Row peersRow) {
    UUID host_id = peersRow.getUUID("host_id");
    String sni = proxyAddress.getHostName().replaceFirst("any", host_id.toString());
    InetSocketAddress proxy = InetSocketAddress.createUnresolved(sni, proxyAddress.getPort());
    return new SniEndPoint(proxy, sni);
  }
}
