/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.connect.common.config;

import java.util.Objects;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;

public class CompressionFragment {

    private static final String GROUP_COMPRESSION = "File Compression";
    private static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";

    private final AbstractConfig cfg;

    public CompressionFragment(final AbstractConfig cfg) {
        this.cfg = cfg;
    }

    public static ConfigDef update(final ConfigDef configDef, final CompressionType defaultCompressionType) {
        configDef.define(FILE_COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING,
                Objects.isNull(defaultCompressionType) ? null : defaultCompressionType.name, // NOPMD NullAssignment
                new FileCompressionTypeValidator(), ConfigDef.Importance.MEDIUM,
                "The compression type used for files put on GCS. " + "The supported values are: "
                        + CompressionType.SUPPORTED_COMPRESSION_TYPES + ".",
                GROUP_COMPRESSION, 1, ConfigDef.Width.NONE, FILE_COMPRESSION_TYPE_CONFIG,
                FixedSetRecommender.ofSupportedValues(CompressionType.names()));
        return configDef;
    }

    public CompressionType getCompressionType() {
        return CompressionType.forName(cfg.getString(FILE_COMPRESSION_TYPE_CONFIG));
    }
}
