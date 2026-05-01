using System.Diagnostics;
using System.IO.Compression;
using System.Text;
using Chapter6.AddressBook;
using Chapter6.Chat;
using Chapter6.DistributedLocks;
using Chapter6.DelayedTasks;
using Chapter6.FileDistribution;
using Chapter6.Threads;
using StackExchange.Redis;

namespace Chapter6;

public class Chapter6 {
	public static void Main() {
		new Chapter6().run();
	}

	private void run() {
		var connection = ConnectionMultiplexer.Connect("localhost");
		var db = connection.GetDatabase(15);
		var server = connection.GetServer("localhost:6379");

		testAddUpdateContact(db);
		testAddressBookAutocomplete(db);
		testDistributedLocking(db);
		testCountingSemaphore(db);
		testDelayedTasks(db);
		testMultiRecipientMessaging(db);
		testFileDistribution(db, server);
	}

	private void testAddUpdateContact(IDatabase db) {
		Console.WriteLine("\n----- testAddUpdateContact -----");
		db.KeyDelete("recent:user");

		Console.WriteLine("Let's add a few contacts...");
		for (var i = 0; i < 10; i++) {
			Contacts.Contacts.AddUpdateContact(db, "user", $"contact-{i / 3}{i}");
		}

		Console.WriteLine("Current recently contacted contacts");
		var contacts = db.ListRange("recent:user", 0, -1);
		foreach (var contact in contacts) {
			Console.WriteLine("  " + contact);
		}

		Debug.Assert(contacts.Length >= 10, "Less than 10 contacts");
		Console.WriteLine();

		Console.WriteLine("Let's pull one of the older ones up to the front");
		Contacts.Contacts.AddUpdateContact(db, "user", "contact-1-4");
		contacts = db.ListRange("recent:user", 0, 2);
		Console.WriteLine("New top-3 contacts:");
		foreach (var contact in contacts) {
			Console.WriteLine("  " + contact);
		}

		Debug.Assert("contact-1-4".Equals(contacts[0]), "contact-1-4 is not the top contact");
		Console.WriteLine();

		Console.WriteLine("Let's remove a contact...");
		Contacts.Contacts.RemoveContact(db, "user", "contact-2-6");
		contacts = db.ListRange("recent:user", 0, -1);
		Console.WriteLine("New contacts:");
		foreach (var contact in contacts) {
			Console.WriteLine("  " + contact);
		}

		Debug.Assert(contacts.Length >= 9, "Contacts are less than 9");
		Console.WriteLine();

		var autocompletePrefix = "contact-2";
		Console.WriteLine($@"And let's finally autocomplete on ""{autocompletePrefix}""");
		Console.WriteLine();
		var all = db.ListRange("recent:user", 0, -1);
		contacts = Contacts.Contacts.FetchAutocompleteList(db, "user", "c");
		Debug.Assert(all.SequenceEqual(contacts), "Contacts and autocomplete list are not equal");
		var equiv = new List<string>();
		foreach (var contact in all) {
			if (contact.StartsWith(autocompletePrefix)) {
				equiv.Add(contact!);
			}
		}

		contacts = Contacts.Contacts.FetchAutocompleteList(db, "user", autocompletePrefix);
		equiv.Sort();

		var contactsList = contacts.Select(x => x.ToString()).ToList();
		contactsList.Sort();

		Console.WriteLine("----Expected autocomplete contacts----");
		foreach (var contact in equiv) {
			Console.WriteLine(contact);
		}

		Console.WriteLine("-----------------------\n");

		Console.WriteLine("----Fetched autocomplete contacts----");
		foreach (var contact in contactsList) {
			Console.WriteLine(contact);
		}

		Console.WriteLine("-----------------------");

		Debug.Assert(equiv.SequenceEqual(contactsList), "Autocomplete failed");
		db.KeyDelete("recent:user");
	}

	private void testAddressBookAutocomplete(IDatabase db) {
		Console.WriteLine("\n----- testAddressBookAutocomplete -----");
		db.KeyDelete("members:test");

		Console.WriteLine($"the start/end range of 'abc' is: [{string.Join(", ", AddressBookOperations.FindPrefixRange("abc"))}]");
		Console.WriteLine();

		Console.WriteLine("Let's add a few people to the guild");
		foreach (var name in new[] { "jeff", "jenny", "jack", "jennifer" }) {
			AddressBookOperations.JoinGuild(db, "test", name);
		}

		Console.WriteLine();
		Console.WriteLine("now let's try to find users with names starting with 'je':");
		var r = AddressBookOperations.AutocompleteOnPrefix(db, "test", "je");
		Console.WriteLine(string.Join(", ", r.ToArray()));
		Debug.Assert(r.Count == 3, "Found usernames are not equal to 3");

		Console.WriteLine("jeff just left to join a different guild... Let's search again!");
		AddressBookOperations.LeaveGuild(db, "test", "jeff");
		r = AddressBookOperations.AutocompleteOnPrefix(db, "test", "je");
		Console.WriteLine(string.Join(", ", r.ToArray()));
		Debug.Assert(r.Count == 2, "Found usernames are not equal to 2");
		db.KeyDelete("members:test");
	}

	private void testDistributedLocking(IDatabase db) {
		Console.WriteLine("\n----- testDistributedLocking -----");

		var lockSpecificName = "testLock";
		var lockFullName = $"lock:{lockSpecificName}";

		db.KeyDelete(lockFullName);
		Console.WriteLine("Getting an initial lock...");
		var distributedLock = DistributedLockOperations.AcquireLockWithTimeout(db, lockSpecificName, 1000, 1000);
		Debug.Assert(distributedLock != null, "Could not acquire lock");
		Console.WriteLine("Got it!");
		Console.WriteLine("Trying to get it again without releasing the first one...");
		var distributedLock2 = DistributedLockOperations.AcquireLockWithTimeout(db, lockSpecificName, 10, 1000);
		Debug.Assert(distributedLock2 is null, "We acquired a lock that we shouldn't have!");
		Console.WriteLine("Failed to get it!");
		Console.WriteLine();

		Console.WriteLine("Waiting for the lock to timeout...");
		Thread.Sleep(2000);
		Console.WriteLine("Getting the lock again...");
		var lockId = DistributedLockOperations.AcquireLockWithTimeout(db, lockSpecificName, 1000, 1000);
		Debug.Assert(lockId is not null, "Failed to reacquire lock!");
		Console.WriteLine("Got it!");
		Console.WriteLine("Releasing the lock...");
		var releaseResult = DistributedLockOperations.ReleaseLock(db, lockSpecificName, lockId);
		Debug.Assert(releaseResult, "Lock was not released!");
		Console.WriteLine("Released it...");
		Console.WriteLine();

		Console.WriteLine("Acquiring it again...");
		var lockId2 = DistributedLockOperations.AcquireLockWithTimeout(db, lockSpecificName, 1000, 1000);
		Debug.Assert(lockId2 is not null, "Failed to get lock. The release probably failed");

		Console.WriteLine("Got it!");
		db.KeyDelete(lockFullName);
	}

	private void testCountingSemaphore(IDatabase db) {
		Console.WriteLine("\n----- testCountingSemaphore -----");
		var semaphoreName = "testSemaphore";
		var ownerSemaphore = $"{semaphoreName}:owner";
		var counterSemaphore = $"{semaphoreName}:counter";
		db.KeyDelete(new RedisKey[] { semaphoreName, ownerSemaphore, counterSemaphore });
		Console.WriteLine("Getting 3 initial semaphores with a limit of 3...");
		for (var i = 0; i < 3; i++) {
			var resultSemaphore = DistributedLockOperations.AcquireFairSemaphore(db, semaphoreName, 3, 1000);
			Debug.Assert(resultSemaphore is not null, "Could not acquire semaphore");
		}

		Console.WriteLine("Done!");
		Console.WriteLine("Getting one more that should fail...");
		var semaphore = DistributedLockOperations.AcquireFairSemaphore(db, semaphoreName, 3, 1000);
		Debug.Assert(semaphore is null, "Shouldn't have acquired a semaphore");
		Console.WriteLine("Couldn't get it!");
		Console.WriteLine();

		Console.WriteLine("Let's wait for some of them to time out");
		Thread.Sleep(2000);
		Console.WriteLine("Can we get one?");
		var id = DistributedLockOperations.AcquireFairSemaphore(db, semaphoreName, 3, 1000);
		Debug.Assert(id is not null, "Did not acquire the semaphore");
		Console.WriteLine("Got one!");
		Console.WriteLine("Let's release it...");
		var releaseResult = DistributedLockOperations.ReleaseFairSemaphore(db, semaphoreName, id);
		Debug.Assert(releaseResult, "Release of semaphore failed!");
		Console.WriteLine("Released!");
		Console.WriteLine();
		Console.WriteLine("And let's make sure we can get 3 more!");
		for (var i = 0; i < 3; i++) {
			var resultSemaphore = DistributedLockOperations.AcquireFairSemaphore(db, semaphoreName, 3, 1000);
			Debug.Assert(resultSemaphore is not null, "Could not acquire semaphore");
		}

		Console.WriteLine("We got them!");
		db.KeyDelete(new RedisKey[] { semaphoreName, ownerSemaphore, counterSemaphore });
	}

	/// <summary>
	/// This code just tests that the queue are updated successfully.
	/// For actual handling of callbacks a different thread object would be required that actually executes the items
	/// </summary>
	private static void testDelayedTasks(IDatabase db) {
		Console.WriteLine("\n----- testDelayedTasks -----");
		var delayedKey = "delayed";
		var queueName = "tqueue";
		var taskQueueKey = $"queue:{queueName}";
		var functionName = "testfn";

		db.KeyDelete(new RedisKey[] {
				taskQueueKey,
				delayedKey
			}
		);

		Console.WriteLine("Let's start some regular and delayed tasks...");
		foreach (var delay in new long[] { 0, 500, 0, 1500 }) {
			var executeLaterResult = DelayedTasksOperations.ExecuteLater(db, queueName, functionName, new List<String>(), delay);
			Debug.Assert(executeLaterResult != null, "Execute later did not return an identifier");
		}

		var scheduledTasksCount = db.ListLength(taskQueueKey);
		Console.WriteLine($"How many non-delayed tasks are there (should be 2)? {scheduledTasksCount}");
		Debug.Assert(scheduledTasksCount == 2, "List is not at the right size");
		Console.WriteLine();

		Console.WriteLine("Let's start up two threads to bring those delayed tasks back...");
		var thread1 = new PollQueueThread(db);
		var thread2 = new PollQueueThread(db);
		thread1.Start();
		thread2.Start();
		Console.WriteLine("Started.");
		Console.WriteLine("Let's wait for those tasks to be prepared...");
		Thread.Sleep(2000);
		thread1.Quit();
		thread2.Quit();
		scheduledTasksCount = db.ListLength(taskQueueKey);
		Console.WriteLine($"Waiting is over, how many tasks do we have (should be 4)? {scheduledTasksCount}");
		Debug.Assert(scheduledTasksCount == 4, "List is not at the right size");

		db.KeyDelete(new RedisKey[] {
				taskQueueKey,
				delayedKey
			}
		);
	}

	private static void testMultiRecipientMessaging(IDatabase db) {
		Console.WriteLine("\n----- testMultiRecipientMessaging -----");
		db.KeyDelete(new RedisKey[] {
			"ids:chat:",
			"msgs:1",
			"ids:1",
			"seen:joe",
			"seen:jeff",
			"seen:jenny"
		});

		Console.WriteLine("Let's create a new chat session with some recipients...");
		var recipients = new HashSet<String>();
		recipients.Add("jeff");
		recipients.Add("jenny");
		var chatId = ChatOperations.CreateChat(db, "joe", recipients, "message 1");
		Console.WriteLine("Now let's send a few messages...");
		for (var i = 2; i < 5; i++) {
			ChatOperations.SendMessage(db, chatId, "joe", "message " + i);
		}

		Console.WriteLine();

		Console.WriteLine("And let's get the messages that are waiting for jeff and jenny...");
		var r1 = ChatOperations.FetchPendingMessages(db, "jeff");
		var r2 = ChatOperations.FetchPendingMessages(db, "jenny");
		Console.WriteLine("They are the same? " + r1.All(r2.Contains));
		Debug.Assert(r1.All(r2.Contains), "Messages are not the same");
		Console.WriteLine("Those messages are:");
		foreach (var chat in r1) {
			Console.WriteLine($"  chatId: {chat.ChatId}");
			Console.WriteLine("    messages:");
			foreach (var message in chat.Messages) {
				var itemToPrint = new StringBuilder();
				itemToPrint.Append(message.Id);
				itemToPrint.Append(',');
				itemToPrint.Append(message.Ts);
				itemToPrint.Append(',');
				itemToPrint.Append(message.Sender);
				itemToPrint.Append(',');
				itemToPrint.Append(message.Message);
				Console.WriteLine("      " + itemToPrint);
			}
		}

		db.KeyDelete(new RedisKey[] {
			"ids:chat:",
			"msgs:1",
			"ids:1",
			"seen:joe",
			"seen:jeff",
			"seen:jenny"
		});
	}

	private static void testFileDistribution(IDatabase db, IServer server) {
		Console.WriteLine("\n----- testFileDistribution -----");
		var ke = server.Keys(15, pattern: "test:*");
		db.KeyDelete(ke.ToArray());

		db.KeyDelete(new RedisKey[] {
			"msgs:test:",
			"seen:0",
			"seen:source",
			"ids:test:",
			"chat:test:"
		});

		Console.WriteLine("Creating some temporary 'log' files...");
		using (var sw1 = new StreamWriter($@"{Path.GetTempPath()}\temp_redis_1_.txt", false)) {
			sw1.WriteLine("one line");
		}

		using (var sw2 = new StreamWriter($@"{Path.GetTempPath()}\temp_redis_2_.txt", false)) {
			for (var i = 0; i < 100; i++) {
				sw2.WriteLine($"many lines {i}");
			}
		}

		if (File.Exists($@"{Path.GetTempPath()}\temp_redis_3_.txt.gz")) {
			File.Delete($@"{Path.GetTempPath()}\temp_redis_3_.txt.gz");
		}

		var f3 = File.Create($@"{Path.GetTempPath()}\temp_redis_3_.txt.gz");
		var size = 0L;
		using (var compress = new GZipStream(f3, CompressionMode.Compress)) {
			using (var sw3 = new StreamWriter(compress)) {
				var random = new Random();
				for (var i = 0; i < 1000; i++) {
					sw3.WriteLine($"random line {random.Next():X10}");
				}
			}

			Console.WriteLine("Done.");
			Console.WriteLine();
			Console.WriteLine("Starting up a thread to copy logs to redis...");
		}

		var compressedFileInfo = new FileInfo($@"{Path.GetTempPath()}\temp_redis_3_.txt.gz");
		size = compressedFileInfo.Length;

		var thread = new CopyLogsThread(db, Path.GetTempPath(), "test:", 1, size);
		thread.Start();

		Console.WriteLine("Let's pause to let some logs get copied to Redis...");
		Thread.Sleep(1000);
		Console.WriteLine();
		Console.WriteLine("Okay, the logs should be ready. Let's process them!");

		Console.WriteLine("Files should have 1, 100, and 1000 lines");
		var callback = new TestCallback();
		ProcessLogsFromRedis.Execute(db, "user_#0", callback);
		var arrayElements = string.Join(",", callback.Counts.Select(c => c.ToString()));
		var displayArray = $"[{arrayElements}]";
		Console.WriteLine(displayArray);
		Debug.Assert(callback.Counts[0] == 1, " File does not have 1 line");
		Debug.Assert(callback.Counts[1] == 100, " File does not have 100 line");
		Debug.Assert(callback.Counts[2] == 1000, " File does not have 1000 line");

		Console.WriteLine();
		Console.WriteLine("Let's wait for the copy thread to finish cleaning up...");
		thread.Quit();
		Console.WriteLine("Done cleaning out Redis!");

		ke = server.Keys(pattern: "test*");
		db.KeyDelete(ke.ToArray());

		db.KeyDelete(new RedisKey[] {
			"msgs:test:",
			"seen:0",
			"seen:source",
			"ids:test:",
			"chat:test:"
		});
	}
}
