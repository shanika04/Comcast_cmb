/**
 * Copyright 2012 Comcast Corporation
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.comcast.cns.model;

/**
 * Exception during model construction
 *
 * @author jorge, bwolf
 */
public class CNSModelConstructionException extends Exception {

  private static final long serialVersionUID = 1L;
  private final String errormessage;

  public CNSModelConstructionException(String errormessage) {
    this.errormessage = errormessage;
  }

  public String getErrormessage() {
    return errormessage;
  }
}
