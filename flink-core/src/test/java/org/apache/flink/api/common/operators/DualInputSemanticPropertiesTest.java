/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.operators;

import org.apache.flink.api.common.operators.util.FieldSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DualInputSemanticPropertiesTest {

    @Test
    void testGetTargetFields() {

        // first input
        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 1);
        sp.addForwardedField(0, 1, 4);
        sp.addForwardedField(0, 2, 3);
        sp.addForwardedField(0, 3, 2);

        assertThat(sp.getForwardingTargetFields(0, 0).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(0, 1).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(0, 2).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(0, 3).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4)).isNotNull();
        assertThat(0).isEqualTo(sp.getForwardingTargetFields(0, 4).size());

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 0);
        sp.addForwardedField(0, 0, 4);
        sp.addForwardedField(0, 1, 1);
        sp.addForwardedField(0, 1, 2);
        sp.addForwardedField(0, 1, 3);

        assertThat(sp.getForwardingTargetFields(0, 0).size()).isEqualTo(2);
        assertThat(sp.getForwardingTargetFields(0, 1).size()).isEqualTo(3);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2)).isNotNull();
        assertThat(sp.getForwardingTargetFields(0, 2).size()).isEqualTo(0);

        // second input
        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 1);
        sp.addForwardedField(1, 1, 4);
        sp.addForwardedField(1, 2, 3);
        sp.addForwardedField(1, 3, 2);

        assertThat(sp.getForwardingTargetFields(1, 0).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(1, 1).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(1, 2).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(1, 3).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(1, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 2).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 3).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 4)).isNotNull();
        assertThat(0).isEqualTo(sp.getForwardingTargetFields(1, 4).size());

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 0);
        sp.addForwardedField(1, 0, 4);
        sp.addForwardedField(1, 1, 1);
        sp.addForwardedField(1, 1, 2);
        sp.addForwardedField(1, 1, 3);

        assertThat(2).isEqualTo(sp.getForwardingTargetFields(1, 0).size());
        assertThat(3).isEqualTo(sp.getForwardingTargetFields(1, 1).size());
        assertThat(sp.getForwardingTargetFields(1, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 2)).isNotNull();
        assertThat(0).isEqualTo(sp.getForwardingTargetFields(1, 2).size());

        // both inputs
        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 2, 6);
        sp.addForwardedField(0, 7, 8);
        sp.addForwardedField(1, 0, 1);
        sp.addForwardedField(1, 1, 4);

        assertThat(sp.getForwardingTargetFields(0, 2).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(0, 7).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(1, 0).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(1, 1).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(0, 2).contains(6)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 7).contains(8)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1)).isNotNull();
        assertThat(sp.getForwardingTargetFields(1, 4)).isNotNull();
        assertThat(0).isEqualTo(sp.getForwardingTargetFields(0, 1).size());
        assertThat(0).isEqualTo(sp.getForwardingTargetFields(1, 4).size());

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 0);
        sp.addForwardedField(0, 0, 4);
        sp.addForwardedField(0, 3, 8);
        sp.addForwardedField(1, 1, 1);
        sp.addForwardedField(1, 1, 2);
        sp.addForwardedField(1, 4, 8);

        assertThat(sp.getForwardingTargetFields(0, 0).size()).isEqualTo(2);
        assertThat(sp.getForwardingTargetFields(0, 3).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(1, 1).size()).isEqualTo(2);
        assertThat(sp.getForwardingTargetFields(1, 4).size()).isEqualTo(1);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(8)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 4).contains(8)).isTrue();
    }

    @Test
    void testGetSourceField() {

        // first input
        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 1);
        sp.addForwardedField(0, 1, 4);
        sp.addForwardedField(0, 2, 3);
        sp.addForwardedField(0, 3, 2);

        assertThat(sp.getForwardingSourceField(0, 1)).isEqualTo(0);
        assertThat(sp.getForwardingSourceField(0, 4)).isEqualTo(1);
        assertThat(sp.getForwardingSourceField(0, 3)).isEqualTo(2);
        assertThat(sp.getForwardingSourceField(0, 2)).isEqualTo(3);
        assertThat(sp.getForwardingSourceField(0, 0) < 0).isTrue();
        assertThat(sp.getForwardingSourceField(0, 5) < 0).isTrue();

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 0);
        sp.addForwardedField(0, 0, 4);
        sp.addForwardedField(0, 1, 1);
        sp.addForwardedField(0, 1, 2);
        sp.addForwardedField(0, 1, 3);

        assertThat(sp.getForwardingSourceField(0, 0)).isEqualTo(0);
        assertThat(sp.getForwardingSourceField(0, 4)).isEqualTo(0);
        assertThat(sp.getForwardingSourceField(0, 1)).isEqualTo(1);
        assertThat(sp.getForwardingSourceField(0, 2)).isEqualTo(1);
        assertThat(sp.getForwardingSourceField(0, 3)).isEqualTo(1);
        assertThat(sp.getForwardingSourceField(0, 5) < 0).isTrue();

        // second input
        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 1);
        sp.addForwardedField(1, 1, 4);
        sp.addForwardedField(1, 2, 3);
        sp.addForwardedField(1, 3, 2);

        assertThat(sp.getForwardingSourceField(1, 1)).isEqualTo(0);
        assertThat(sp.getForwardingSourceField(1, 4)).isEqualTo(1);
        assertThat(sp.getForwardingSourceField(1, 3)).isEqualTo(2);
        assertThat(sp.getForwardingSourceField(1, 2)).isEqualTo(3);
        assertThat(sp.getForwardingSourceField(1, 0) < 0).isTrue();
        assertThat(sp.getForwardingSourceField(1, 5) < 0).isTrue();

        sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 0);
        sp.addForwardedField(1, 0, 4);
        sp.addForwardedField(1, 1, 1);
        sp.addForwardedField(1, 1, 2);
        sp.addForwardedField(1, 1, 3);

        assertThat(sp.getForwardingSourceField(1, 0)).isEqualTo(0);
        assertThat(sp.getForwardingSourceField(1, 4)).isEqualTo(0);
        assertThat(sp.getForwardingSourceField(1, 1)).isEqualTo(1);
        assertThat(sp.getForwardingSourceField(1, 2)).isEqualTo(1);
        assertThat(sp.getForwardingSourceField(1, 3)).isEqualTo(1);
        assertThat(sp.getForwardingSourceField(1, 5) < 0).isTrue();
    }

    @Test
    void testGetReadSet() {

        // first input
        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addReadFields(0, new FieldSet(0, 1));

        assertThat(2).isEqualTo(sp.getReadFields(0).size());
        assertThat(sp.getReadFields(0).contains(0)).isTrue();
        assertThat(sp.getReadFields(0).contains(1)).isTrue();

        sp.addReadFields(0, new FieldSet(3));

        assertThat(3).isEqualTo(sp.getReadFields(0).size());
        assertThat(sp.getReadFields(0).contains(0)).isTrue();
        assertThat(sp.getReadFields(0).contains(1)).isTrue();
        assertThat(sp.getReadFields(0).contains(3)).isTrue();

        // second input
        sp = new DualInputSemanticProperties();
        sp.addReadFields(1, new FieldSet(0, 1));

        assertThat(2).isEqualTo(sp.getReadFields(1).size());
        assertThat(sp.getReadFields(1).contains(0)).isTrue();
        assertThat(sp.getReadFields(1).contains(1)).isTrue();

        sp.addReadFields(1, new FieldSet(3));

        assertThat(3).isEqualTo(sp.getReadFields(1).size());
        assertThat(sp.getReadFields(1).contains(0)).isTrue();
        assertThat(sp.getReadFields(1).contains(1)).isTrue();
        assertThat(sp.getReadFields(1).contains(3)).isTrue();
    }

    @Test
    public void testAddForwardedFieldsTargetTwice1() {

        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addForwardedField(0, 0, 2);
        assertThatThrownBy(() -> sp.addForwardedField(0, 1, 2))
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }

    @Test
    public void testAddForwardedFieldsTargetTwice2() {

        DualInputSemanticProperties sp = new DualInputSemanticProperties();
        sp.addForwardedField(1, 0, 2);
        assertThatThrownBy(() -> sp.addForwardedField(1, 1, 2))
                .isInstanceOf(SemanticProperties.InvalidSemanticAnnotationException.class);
    }
}
