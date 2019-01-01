/*
 * Copyright (c) 2018. cloudscheduler
 * All rights reserved.
 *
 * Permission is hereby granted, free  of charge, to any person obtaining
 * a  copy  of this  software  and  associated  documentation files  (the
 * "Software"), to  deal in  the Software without  restriction, including
 * without limitation  the rights to  use, copy, modify,  merge, publish,
 * distribute,  sublicense, and/or sell  copies of  the Software,  and to
 * permit persons to whom the Software  is furnished to do so, subject to
 * the following conditions:
 *
 * The  above  copyright  notice  and  this permission  notice  shall  be
 * included in all copies or substantial portions of the Software.
 *
 * THE  SOFTWARE IS  PROVIDED  "AS  IS", WITHOUT  WARRANTY  OF ANY  KIND,
 * EXPRESS OR  IMPLIED, INCLUDING  BUT NOT LIMITED  TO THE  WARRANTIES OF
 * MERCHANTABILITY,    FITNESS    FOR    A   PARTICULAR    PURPOSE    AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE,  ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package io.github.cloudscheduler.codec;

import java.util.ServiceLoader;

/**
 * <p>Entity Codec Provider. Use {@link ServiceLoader} feature to load implementation.
 * Register implementation in
 * /META-INF/services/io.github.cloudscheduler.codec.EntityCodecProvider</p>
 *
 * <p>Cloudscheduler provide jackson based implementation and protobuf based implementation,
 * if you want use them, please add cloudscheduler-codec-json or cloudscheduler-codec-protobuf
 * as your dependency</p>
 *
 * <p>If jackson (version 2) conflict with your environment, you can use protobuf implementation
 * or implement your own codec provider</p>
 *
 * <p>Please note: YOU SHOULD REGISTER ONLY ONE CODEC PROVIDER</p>
 *
 * @author Wei Gao
 */
public interface EntityCodecProvider {
  /**
   * Getting codec provider by service loader.
   *
   * @return entity code provider
   */
  static EntityCodecProvider getCodecProvider() {
    ServiceLoader<EntityCodecProvider> serviceLoader =
        ServiceLoader.load(EntityCodecProvider.class);
    if (serviceLoader.iterator().hasNext()) {
      return serviceLoader.iterator().next();
    } else {
      throw new IllegalArgumentException("Cannot find suitable entity codec provider");
    }
  }

  <T> EntityCodec<T> getEntityCodec(Class<T> type);

  default <T> EntityDecoder<T> getEntityDecoder(Class<T> type) {
    return getEntityCodec(type);
  }

  default <T> EntityEncoder<T> getEntityEncoder(Class<T> type) {
    return getEntityCodec(type);
  }
}
