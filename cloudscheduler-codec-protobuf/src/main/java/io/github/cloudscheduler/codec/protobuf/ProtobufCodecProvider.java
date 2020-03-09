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

package io.github.cloudscheduler.codec.protobuf;

import io.github.cloudscheduler.codec.EntityCodec;
import io.github.cloudscheduler.codec.EntityCodecProvider;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.util.HashMap;
import java.util.Map;

public class ProtobufCodecProvider implements EntityCodecProvider {
  private final Map<Class, EntityCodec> codeces = new HashMap<>();

  @Override
  @SuppressWarnings("unchecked")
  public <T> EntityCodec<T> getEntityCodec(Class<T> type) {
    EntityCodec<T> codec = codeces.get(type);
    if (codec == null) {
      codeces.putIfAbsent(type, new ProtobufCodec(RuntimeSchema.getSchema(type)));
    }
    codec = codeces.get(type);
    return codec;
  }

  private static final class ProtobufCodec<T> implements EntityCodec<T> {
    private final Schema<T> schema;

    private ProtobufCodec(Schema<T> schema) {
      this.schema = schema;
    }

    @Override
    public T decode(byte[] data) {
      T entity = schema.newMessage();
      ProtobufIOUtil.mergeFrom(data, entity, schema);
      return entity;
    }

    @Override
    public byte[] encode(T entity) {
      LinkedBuffer buffer = LinkedBuffer.allocate();
      try {
        return ProtobufIOUtil.toByteArray(entity, schema, buffer);
      } finally {
        buffer.clear();
      }
    }
  }
}
