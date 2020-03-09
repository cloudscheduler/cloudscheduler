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

package io.github.cloudscheduler.codec.jackson;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import io.github.cloudscheduler.Node;
import io.github.cloudscheduler.codec.EntityCodec;
import io.github.cloudscheduler.codec.EntityCodecProvider;
import io.github.cloudscheduler.model.JobDefinition;
import io.github.cloudscheduler.model.JobDefinitionStatus;
import io.github.cloudscheduler.model.JobInstance;
import io.github.cloudscheduler.model.JobRunStatus;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Jackson based codec provider.
 *
 * @author Wei Gao
 */
public class JacksonCodecProvider implements EntityCodecProvider {
  private final ObjectMapper objectMapper;
  private final Map<Class<?>, EntityCodec<?>> codecs;

  /**
   * Default constructor.
   */
  public JacksonCodecProvider() {
    objectMapper = new ObjectMapper()
        .registerModule(new ParameterNamesModule())
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule())
        .setSerializationInclusion(NON_EMPTY);
    objectMapper.addMixIn(JobDefinition.class, JobDefinitionMixIn.class);
    objectMapper.addMixIn(JobDefinitionStatus.class, JobDefinitionStatusMixIn.class);
    objectMapper.addMixIn(JobInstance.class, JobInstanceMixIn.class);
    objectMapper.addMixIn(JobRunStatus.class, JobRunStatusMixIn.class);
    objectMapper.addMixIn(Node.class, NodeMixIn.class);

    codecs = new HashMap<>();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> EntityCodec<T> getEntityCodec(Class<T> type) {
    EntityCodec<T> codec = (EntityCodec<T>) codecs.get(type);
    if (codec == null) {
      synchronized (codecs) {
        codec = (EntityCodec<T>) codecs.get(type);
        if (codec == null) {
          codec = new EntityCodec<T>() {
            @Override
            public T decode(byte[] data) throws IOException {
              if (data == null) {
                return null;
              } else {
                return objectMapper.readValue(data, type);
              }
            }

            @Override
            public byte[] encode(T entity) throws IOException {
              if (entity == null) {
                return null;
              } else {
                return objectMapper.writeValueAsBytes(entity);
              }
            }
          };
          codecs.put(type, codec);
        }
      }
    }
    return codec;
  }
}
