/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.file.io.jaydio;

import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;

import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.file.io.IoBuffer;

public class JaydioDirectByteBuffer implements IoBuffer {

	private AlignedDirectByteBuffer byteBuffer;
	
	public JaydioDirectByteBuffer(int capacity) {
		byteBuffer = AlignedDirectByteBuffer
				.allocate(DirectIoLib.getLibForPath(FileMgr.DB_FILES_DIR), capacity);
	}
	
	@Override
	public IoBuffer get(int position, byte[] dst) {
		byteBuffer.position(position);
		byteBuffer.get(dst);
		return this;
	}

	@Override
	public IoBuffer put(int position, byte[] src) {
		byteBuffer.position(position);
		byteBuffer.put(src);
		return this;
	}
	
	@Override
	public void clear() {
		byteBuffer.clear();
	}

	@Override
	public void rewind() {
		byteBuffer.rewind();
	}

	@Override
	public void close() {
		byteBuffer.close();
	}
	
	AlignedDirectByteBuffer getAlignedDirectByteBuffer() {
		return byteBuffer;
	}
}
