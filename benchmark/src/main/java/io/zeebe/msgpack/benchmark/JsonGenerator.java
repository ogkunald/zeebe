/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
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
 */
package io.zeebe.msgpack.benchmark;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

public class JsonGenerator
{

    protected int maxLevel;
    protected int numKeysPerLevel;

    public JsonGenerator(int maxLevel, int numKeysPerLevel)
    {
        this.maxLevel = maxLevel;
        this.numKeysPerLevel = numKeysPerLevel;
    }

    public void generate(OutputStream outStream) throws Exception
    {
        final OutputStreamWriter outWriter = new OutputStreamWriter(outStream, StandardCharsets.UTF_8);

        final int numLeafElements = (int) Math.pow(numKeysPerLevel, maxLevel + 1);

        int currentLevel = 0;
        outWriter.write("{");
        for (int i = 0; i < numLeafElements; i++)
        {
            while (currentLevel < maxLevel)
            {
                final int offsetOnLevel = offsetOnLevel(i, currentLevel, maxLevel, numKeysPerLevel);
                if (offsetOnLevel > 0)
                {
                    outWriter.append(",");
                }

                outWriter.write("\"");
                outWriter.write((char) (offsetOnLevel + 65));
                outWriter.write("\"");
                outWriter.write(":");
                outWriter.write("{");
                currentLevel++;
            }

            final int offsetOnLevel = offsetOnLevel(i, currentLevel, maxLevel, numKeysPerLevel);

            outWriter.write("\"");
            outWriter.write((char) (offsetOnLevel + 65));
            outWriter.write("\"");
            outWriter.write(":");
            outWriter.write(Integer.toString(i));

            if (offsetOnLevel < numKeysPerLevel - 1)
            {
                outWriter.write(",");
            }

            while (offsetOnLevel(i, currentLevel, maxLevel, numKeysPerLevel) == numKeysPerLevel - 1 && currentLevel > 0)
            {
                outWriter.write("}");
                currentLevel--;
            }

        }
        outWriter.write("}");
        outWriter.flush();

        outStream.flush();
    }

    public static void main(String[] args)
    {

    }

    protected static int offsetOnLevel(int index, int level, int maxLevel, int numKeysPerLevel)
    {
        final int stepSize = (int) Math.pow(numKeysPerLevel, maxLevel - level);
        final int parentStepSize = stepSize * numKeysPerLevel;
        return (index % parentStepSize) / stepSize;
    }

    protected static String indexToString(int index)
    {
        return new String(new byte[]{ (byte) (index + 65) }, StandardCharsets.UTF_8);
    }
}
