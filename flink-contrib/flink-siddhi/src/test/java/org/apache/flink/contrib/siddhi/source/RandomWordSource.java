/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomWordSource implements SourceFunction<String> {
	private static final String[] WORDS = new String[] {
		"To be, or not to be,--that is the question:--",
		"Whether 'tis nobler in the mind to suffer",
		"The slings and arrows of outrageous fortune",
		"Or to take arms against a sea of troubles,",
		"And by opposing end them?--To die,--to sleep,--",
		"No more; and by a sleep to say we end",
		"The heartache, and the thousand natural shocks",
		"That flesh is heir to,--'tis a consummation",
		"Devoutly to be wish'd. To die,--to sleep;--",
		"To sleep! perchance to dream:--ay, there's the rub;",
		"For in that sleep of death what dreams may come,",
		"When we have shuffled off this mortal coil,",
		"Must give us pause: there's the respect",
		"That makes calamity of so long life;",
		"For who would bear the whips and scorns of time,",
		"The oppressor's wrong, the proud man's contumely,",
		"The pangs of despis'd love, the law's delay,",
		"The insolence of office, and the spurns",
		"That patient merit of the unworthy takes,",
		"When he himself might his quietus make",
		"With a bare bodkin? who would these fardels bear,",
		"To grunt and sweat under a weary life,",
		"But that the dread of something after death,--",
		"The undiscover'd country, from whose bourn",
		"No traveller returns,--puzzles the will,",
		"And makes us rather bear those ills we have",
		"Than fly to others that we know not of?",
		"Thus conscience does make cowards of us all;",
		"And thus the native hue of resolution",
		"Is sicklied o'er with the pale cast of thought;",
		"And enterprises of great pith and moment,",
		"With this regard, their currents turn awry,",
		"And lose the name of action.--Soft you now!",
		"The fair Ophelia!--Nymph, in thy orisons",
		"Be all my sins remember'd."
	};

	private final int count;
	private final Random random;
	private final long initialTimestamp;

	private volatile boolean isRunning = true;
	private volatile int number = 0;
	private long closeDelayTimestamp;

	public RandomWordSource(int count, long initialTimestamp) {
		this.count = count;
		this.random = new Random();
		this.initialTimestamp = initialTimestamp;
	}

	public RandomWordSource() {
		this(Integer.MAX_VALUE,System.currentTimeMillis());
	}

	public RandomWordSource(int count) {
		this(count,System.currentTimeMillis());
	}


	public RandomWordSource closeDelay(long delayTimestamp){
		this.closeDelayTimestamp = delayTimestamp;
		return this;
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (isRunning) {
			ctx.collectWithTimestamp(WORDS[random.nextInt(WORDS.length)],initialTimestamp+1000*number);
			number++;
			if (number >= this.count) {
				cancel();
			}
		}
	}

	@Override
	public void cancel() {
		this.isRunning = false;
		try {
			Thread.sleep(this.closeDelayTimestamp);
		} catch (InterruptedException e) {
			// ignored
		}
	}
}
