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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import io.github.cloudscheduler.util.UuidUtils;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class EntityCodecTest {
  @Test
  public void testGetEntityCodecProvider() {
    EntityCodecProvider provider = EntityCodecProvider.getCodecProvider();
    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(EntityCodecProviderTest.class);
  }

  @Test
  public void testGetEntityCodec() {
    EntityCodecProvider provider = EntityCodecProvider.getCodecProvider();
    assertThat(provider).isNotNull();
    EntityCodec<TestEntity> codec = provider.getEntityCodec(TestEntity.class);
    assertThat(codec).isNotNull();
  }

  @Test
  public void testGetEntityCodecForUnknown() {
    EntityCodecProvider provider = EntityCodecProvider.getCodecProvider();
    assertThat(provider).isNotNull();
    assertThatIllegalStateException().isThrownBy(() -> provider.getEntityCodec(Object.class));
  }

  @Test
  public void testEntityCodec() throws IOException {
    EntityCodecProvider provider = EntityCodecProvider.getCodecProvider();
    assertThat(provider).isNotNull();
    EntityCodec<TestEntity> codec = provider.getEntityCodec(TestEntity.class);
    assertThat(codec).isNotNull();
    UUID id = UUID.randomUUID();
    TestEntity entity = new TestEntity(id);

    byte[] data = codec.encode(entity);
    assertThat(data).isNotEmpty().hasSize(16);

    TestEntity entity1 = codec.decode(data);
    assertThat(entity1).isNotNull();
    assertThat(entity1.getId()).isEqualTo(id);
  }

  public static class EntityCodecProviderTest implements EntityCodecProvider {
    @Override
    @SuppressWarnings("unchecked")
    public <T> EntityCodec<T> getEntityCodec(Class<T> type) {
      if (TestEntity.class.isAssignableFrom(type)) {
        return (EntityCodec<T>) new TestEntityCodec();
      } else {
        throw new IllegalStateException("Do not recognize entity type.");
      }
    }
  }

  private static class TestEntityCodec implements EntityCodec<TestEntity> {

    @Override
    public TestEntity decode(byte[] data) {
      if (data == null) {
        return null;
      }
      UUID id = UuidUtils.asUuid(data);
      return new TestEntity(id);
    }

    @Override
    public byte[] encode(TestEntity entity) {
      if (entity == null) {
        return null;
      }
      return UuidUtils.asBytes(entity.getId());
    }
  }

  private static class TestEntity {
    private final UUID id;

    TestEntity(UUID id) {
      Objects.requireNonNull(id);
      this.id = id;
    }

    public UUID getId() {
      return id;
    }
  }
}
