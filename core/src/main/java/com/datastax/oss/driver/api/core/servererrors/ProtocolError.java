/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.servererrors;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;

/**
 * Indicates that the contacted node reported a protocol error.
 *
 * <p>Protocol errors indicate that the client triggered a protocol violation (for instance, a
 * {@code QUERY} message is sent before a {@code STARTUP} one has been sent). Protocol errors should
 * be considered as a bug in the driver and reported as such.
 *
 * <p>This exception does not go through the {@link RetryPolicy}, it is always rethrown directly to
 * the client.
 */
public class ProtocolError extends CoordinatorException {

  public ProtocolError(Node coordinator, String message) {
    this(coordinator, message, false);
  }

  private ProtocolError(Node coordinator, String message, boolean writableStackTrace) {
    super(coordinator, message, writableStackTrace);
  }

  @Override
  public DriverException copy() {
    return new ProtocolError(getCoordinator(), getMessage(), true);
  }
}