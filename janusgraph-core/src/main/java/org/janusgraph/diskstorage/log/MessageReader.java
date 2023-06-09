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

/**
 * Implementations of this interface are used to process messages read from the log.
 * 用来从log中读取消息处理。
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface MessageReader {

    /**
     * Processes the given message. The message object may not be mutated!
     * @param message
     */
    void read(Message message);

    /**
     * Updates the state of the MessageReader.
     */
    public void updateState();

    /**
     * Need to override this method because the {@link Log} uses this comparison
     * when un-registering readers
     *
     * @param other other reader to compare against
     * @return
     */
    @Override
    boolean equals(Object other);

}
