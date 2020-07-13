/**
 * Copyright (C) 2020 Expedia, Inc.
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
package com.expediagroup.dataplatform.dronefly.app.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryListenerEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessageReader;

@ExtendWith(MockitoExtension.class)
public class MessageReaderAdapterTest {

  private @Mock KafkaMessageReader delegate;
  private @Mock ApiaryListenerEvent event;
  private MessageReaderAdapter messageReaderAdapter;

  @BeforeEach
  public void init() {
    messageReaderAdapter = new MessageReaderAdapter(delegate);
  }

  @Test
  public void typicalRead() {
    when(delegate.next()).thenReturn(event);
    ApiaryListenerEvent result = messageReaderAdapter.read();
    verify(delegate).next();
    assertThat(result).isEqualTo(event);
  }

  @Test
  public void typicalClose() throws IOException {
    messageReaderAdapter.close();
    verify(delegate).close();
  }

}
