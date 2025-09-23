# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import namedtuple
from typing import List, Any, Optional


# Create immutable MessageBatch value object
MessageBatch = namedtuple('MessageBatch', [
    'topic',        # str: Target topic for this batch
    'messages',     # tuple: Prepared message dictionaries with callbacks
    'futures',      # tuple: asyncio.Future objects to resolve
])


def create_message_batch(topic: str, messages: List[dict], futures: List[Any], callbacks: Optional[Any] = None) -> MessageBatch:
    """Create an immutable MessageBatch from lists
    
    This factory function converts mutable lists into an immutable MessageBatch object.
    
    Args:
        topic: Target topic name
        messages: List of prepared message dictionaries
        futures: List of asyncio.Future objects
        callbacks: Deprecated parameter, ignored for backwards compatibility
        
    Returns:
        MessageBatch: Immutable batch object
    """
    return MessageBatch(
        topic=topic,
        messages=tuple(messages),
        futures=tuple(futures)
    )


# Add convenience properties to MessageBatch
def _get_batch_size(self) -> int:
    """Get the number of messages in this batch"""
    return len(self.messages)

def _get_batch_info(self) -> str:
    """Get a string representation of batch info"""
    return f"MessageBatch(topic='{self.topic}', size={len(self.messages)})"

# Monkey-patch methods onto the namedtuple
MessageBatch.size = property(_get_batch_size)
MessageBatch.info = property(_get_batch_info)
