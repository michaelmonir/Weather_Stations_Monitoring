package com.example.bitcask;

import com.example.bitcask.Bitcask.Bitcask;
import com.example.bitcask.Bitcask.BitcaskRunner;
import com.example.bitcask.Exceptions.IdDoesNotExistException;
import com.example.bitcask.Message.Message;
import com.example.bitcask.NewMerge.MergeScheduler;
import com.example.bitcask.NewMerge.Merger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.FileNotFoundException;
import java.io.IOException;

@SpringBootTest
class BitcaskApplicationTests {

	@Test
	public void testContinuousMerge() {
		BitcaskRunner.start(10, 15L);

		for (int i = 0; i < 1000; i++) {
			try {
				Thread.sleep(10L);
			} catch (InterruptedException e) {
			}
			BitcaskRunner.put((long)i);
		}
	}

	@Test
	public void testContinuousMerge2() {
		BitcaskRunner.start(10, 15L);

		for (int i = 0; i < 100; i++) {
			BitcaskRunner.put(i);
			Merger merger = new Merger();
			merger.run();
		}
		for (int i = 0; i < 100; i++) {
			Message message = BitcaskRunner.read((long)i);
			Assertions.assertEquals(i, message.getS_no());
		}
	}

	@Test
	public void withoutMergeAndLargeFiles() {
		BitcaskRunner.start(1_000_000, 15L);

		for (int i = 0; i < 100_000; i++) {
			BitcaskRunner.put(i);
		}
		for (int i = 0; i < 100_000; i++) {
			Message message = BitcaskRunner.read(i);
			Assertions.assertEquals(i, message.getS_no());
		}
	}

	@Test
	public void mergeAfter100() {
		BitcaskRunner.start(1_000_000, 15L);

		for (int i = 0; i < 100_000; i++) {
			BitcaskRunner.put(i);
		}
		for (int i = 0; i < 100_000; i++) {
			Message message = BitcaskRunner.read(i);
			Assertions.assertEquals(i, message.getS_no());
		}
	}

	@Test
	public void testSchedulerResume() {
		BitcaskRunner.start(10, 100_000L);
		for (int i = 0; i < 100; i++) {
			BitcaskRunner.put(i);
		}
		Assertions.assertEquals(100, Bitcask.getBitcask().getSegments().size());
		MergeScheduler.resume();
		try {
			Thread.sleep(2_000L);
		} catch (InterruptedException e) {
		}
		Assertions.assertNotEquals(100, Bitcask.getBitcask().getSegments().size());
	}

	@Test
	public void addWithGetConcurrentSmallFiles() {
		BitcaskRunner.start(10, 1_000_000_000L);
		Thread thread1 = new Thread(() -> {
			for (int i = 0; i < 10; i++) {
				BitcaskRunner.put(i);
			}
		});
		Thread thread2 = new Thread(() -> {
			for (int i = 0; i < 10; i++) {
				try {
					Message message = BitcaskRunner.read(i);
					Assertions.assertEquals(i, message.getS_no());
				} catch (IdDoesNotExistException e) {
				}
			}
		});
		thread1.start();
		thread2.start();
		try {
			thread1.join();
			thread2.join();
		} catch (InterruptedException e) {
		}
	}

	@Test
	public void addWithGetConcurrentBigFilesFiles() {
		BitcaskRunner.start(100_000 * 38, 1_000_000_000L);
		Thread thread1 = new Thread(() -> {
			for (int i = 0; i < 1_000_000; i++) {
				BitcaskRunner.put(i);
			}
		});
		Thread thread2 = new Thread(() -> {
			for (int i = 0; i < 1_000_000; i++) {
				try {
					Message message = BitcaskRunner.read(i);
					Assertions.assertEquals(i, message.getS_no());
				} catch (IdDoesNotExistException e) {
				}
			}
		});
		thread1.start();
		thread2.start();
		try {
			thread1.join();
			thread2.join();
		} catch (InterruptedException e) {
		}
	}

	@Test
	public void addWithGetwithMergeConcurrentBigFilesFiles() {
		BitcaskRunner.start(1_000 * 38, 100L);
		Thread thread1 = new Thread(() -> {
			for (int i = 0; i < 1_000_000; i++) {
				int random = (int) (Math.random() * 1000);
				BitcaskRunner.put(random);
			}
		});
		Thread thread2 = new Thread(() -> {
			for (int i = 0; i < 1_000_000; i++) {
				try {
					int random = (int) (Math.random() * 1000);
					Message message = BitcaskRunner.read(random);
					Assertions.assertEquals(random, message.getS_no());
				} catch (IdDoesNotExistException e) {
				}
			}
		});

		thread1.start();
		thread2.start();
		try {
			thread1.join();
			thread2.join();
		} catch (InterruptedException e) {
		}
	}

	@Test
	public void testBasicRecovery() {
		BitcaskRunner.start(1, 1000_000L);
		for (int i = 0; i < 1; i++)
			BitcaskRunner.put(i);
		for (int i = 0; i < 1; i++) {
			Message message = BitcaskRunner.read(i);
			Assertions.assertEquals(message.getS_no(), i);
		}

		BitcaskRunner.startAndRecover(1, 1000_000L);

		for (int i = 0; i < 1; i++) {
			Message message = BitcaskRunner.read(i);
			Assertions.assertEquals(message.getS_no(), i);
		}
	}
}

