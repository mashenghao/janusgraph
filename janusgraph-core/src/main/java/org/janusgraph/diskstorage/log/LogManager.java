// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.log;

import org.janusgraph.diskstorage.BackendException;

/**
 * Manager interface for opening {@link Log}s against a particular Log implementation.
 * 针对特定日志 管理接口
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface LogManager {

    /**
     * Opens a log for the given name.
     * <p>
     * If a log with the given name already exists, the existing log is returned.
     *
     * @param name Name of the log to be opened
     * @return
     * @throws org.janusgraph.diskstorage.BackendException
     */
    Log openLog(String name) throws BackendException;

    /**
     * Closes the log manager and all open logs (if they haven't already been explicitly closed)
     *
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void close() throws BackendException;

}
