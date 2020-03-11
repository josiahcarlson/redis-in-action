package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"redisInAction/Chapter06/common"
	"redisInAction/Chapter06/model"
	"redisInAction/redisConn"
	"redisInAction/utils"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func Test(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)

	t.Run("Test add update contact", func(t *testing.T) {
		t.Log("Let's add a few contacts...")
		for i := 0; i < 10; i++ {
			client.AddUpdateContact("user", fmt.Sprintf("contact-%d-%d", i/3, i))
		}
		t.Log("Current recently contacted contacts")
		contacts := client.Conn.LRange("recent:user", 0, -1).Val()
		for _, v := range contacts {
			t.Log(v)
		}
		utils.AssertTrue(t, len(contacts) >= 10)

		t.Log("Let's pull one of the older ones up to the front")
		client.AddUpdateContact("user", "contact-1-4")
		contacts = client.Conn.LRange("recent:user", 0, 2).Val()
		t.Log("New top-3 contacts:")
		for _, v := range contacts {
			t.Log(v)
		}
		utils.AssertTrue(t, contacts[0] == "contact-1-4")

		t.Log("Let's remove a contact...")
		client.RemoveContact("user", "contact-2-6")
		contacts = client.Conn.LRange("recent:user", 0, -1).Val()
		t.Log("New contacts:")
		for _, v := range contacts {
			t.Log(v)
		}
		utils.AssertTrue(t, len(contacts) >= 9)

		t.Log("And let's finally autocomplete on ")
		all := client.Conn.LRange("recent:user", 0, -1).Val()
		contacts = client.FetchAutoCompleteList("user", "c")
		utils.AssertTrue(t, reflect.DeepEqual(all, contacts))
		equiv := []string{}
		for _, v := range all {
			if strings.HasPrefix(v, "contact-2-") {
				equiv = append(equiv, v)
			}
		}
		contacts = client.FetchAutoCompleteList("user", "contact-2-")
		utils.AssertTrue(t, reflect.DeepEqual(equiv, contacts))
		defer client.Conn.FlushDB()
	})

	t.Run("Test address book autocomplete", func(t *testing.T) {
		t.Log("the start/end range of 'abc' is:")
		start, end := client.FindPrefixRange("abc")
		t.Log(start, end)

		t.Log("Let's add a few people to the guild")
		for _, name := range []string{"jeff", "jenny", "jack", "jennifer"} {
			client.JoinGuild("test", name)
		}
		t.Log("now let's try to find users with names starting with 'je':")
		r := client.AutoCompleteOnPrefix("test", "je")
		t.Log(r)
		defer client.Conn.FlushDB()
	})

	t.Run("Test distributed locking", func(t *testing.T) {
		t.Log("Getting an initial lock...")
		utils.AssertTrue(t, client.AcquireLockWithTimeout("testlock", 1, 1) != "")
		t.Log("Got it!")
		t.Log("Trying to get it again without releasing the first one...")
		utils.AssertFalse(t, client.AcquireLockWithTimeout("testlock", 0.01, 1) != "")
		t.Log("Failed to get it!")
		t.Log("Waiting for the lock to timeout...")
		time.Sleep(1 * time.Second)
		t.Log("Getting the lock again...")
		r := client.AcquireLockWithTimeout("testlock", 1, 1)
		utils.AssertTrue(t, r != "")
		t.Log("Got it!")
		t.Log("Releasing the lock...")
		utils.AssertTrue(t, client.ReleaseLock("testlock", r))
		t.Log("Released it...")
		t.Log("Acquiring it again...")
		utils.AssertTrue(t, client.AcquireLockWithTimeout("testlock", 1, 1) != "")
		t.Log("Got it!")
		defer client.Conn.FlushDB()
	})

	t.Run("Test counting semaphore", func(t *testing.T) {
		t.Log("Getting 3 initial semaphores with a limit of 3...")
		for i := 0; i < 3; i++ {
			utils.AssertTrue(t, client.AcquireFairSemaphore("testsem", 3, 1) != "")
		}
		t.Log("Done!")
		t.Log("Getting one more that should fail...")
		utils.AssertFalse(t, client.AcquireFairSemaphore("testsem", 3, 1) != "")
		t.Log("Couldn't get it!")
		t.Log("Lets's wait for some of them to time out")
		time.Sleep(2 * time.Second)
		t.Log("Can we get one?")
		r := client.AcquireFairSemaphore("testsem", 3, 1)
		utils.AssertTrue(t, r != "")
		t.Log("Got one!")
		t.Log("Let's release it...")
		utils.AssertTrue(t, client.ReleaseFairSemaphore("testsem", r))
		t.Log("Released!")
		t.Log("And let's make sure we can get 3 more!")
		for i := 0; i < 3; i++ {
			utils.AssertTrue(t, client.AcquireFairSemaphore("testsem", 3, 1) != "")
		}
		t.Log("We got them!")
		defer client.Conn.FlushDB()
	})

	t.Run("Test delayed tasks", func(t *testing.T) {
		t.Log("Let's start some regular and delayed tasks...")
		for _, delay := range []float64{0, 0.5, 0, 1.5} {
			utils.AssertTrue(t, client.ExecuteLater("tqueue", "testfn", nil, delay) != "")
		}
		r := client.Conn.LLen("queue:tqueue").Val()
		t.Log("How many non-delayed tasks are there (should be 2)?", r)
		utils.AssertnumResult(t, 2, r)
		t.Log("Let's start up a thread to bring those delayed tasks back...")
		channel := make(chan struct{})
		go client.PollQueue(channel)
		t.Log("Started.")
		t.Log("Let's wait for those tasks to be prepared...")
		time.Sleep(2 * time.Second)
		common.QUIT = true
		<-channel
		r = client.Conn.LLen("queue:tqueue").Val()
		t.Log("Waiting is over, how many tasks do we have (should be 4)?", r)
		utils.AssertnumResult(t, 4, r)
		defer client.Conn.FlushDB()
	})

	t.Run("Test multi recipient messaging", func(t *testing.T) {
		t.Log("Let's create a new chat session with some recipients...")
		chatId := client.CreateChat("joe", &[]string{"jeff", "jenny"}, "message 1", "")
		t.Log("Now let's send a few messages...")
		for i := 2; i < 5; i++ {
			client.SendMessage(chatId, "joe", fmt.Sprintf("message %s", strconv.Itoa(i)))
		}
		t.Log("And let's get the messages that are waiting for jeff and jenny...")
		r1 := client.FetchPendingMessage("jeff")
		r2 := client.FetchPendingMessage("jenny")
		t.Log("They are the same?", reflect.DeepEqual(r1, r2))
		utils.AssertTrue(t, reflect.DeepEqual(r1, r2))
		t.Log("Those messages are:", r1)
		defer client.Conn.FlushDB()
	})
	
	t.Run("Test file distribution", func(t *testing.T) {
		tempDirPath, err := ioutil.TempDir("", "myTempDir")
		if err != nil {
			log.Fatal(err)
		}

		t.Log("Creating some temporary 'log' files...")
		sizes := []int64{}
		tempFile1 := utils.GenerationFile(tempDirPath, "temp-1.txt", "one line\n")
		s1, _ := tempFile1.Seek(0, 1)
		tempFile2 := utils.GenerationFile(tempDirPath, "temp-2.txt",
			strings.Repeat("many lines\n", 10000))
		s2, _ := tempFile2.Seek(0, 1)
		outFile := utils.GenerationZipFile(tempDirPath, "temp-3.gz")
		s3, _ := outFile.Seek(0, 1)
		sizes = append(sizes, s1, s2, s3)
		t.Log("Done.")

		t.Log("Starting up a thread to copy logs to redis...")
		var sum int64 = 0
		for _, v := range sizes {
			sum += v
		}
		go client.CopyLogsToRedis(tempDirPath, "test:", 1, sum + 1, true)

		t.Log("Let's pause to let some logs get copied to Redis...")
		time.Sleep(250 * time.Millisecond)

		t.Log("Okay, the logs should be ready. Let's process them!")

		index := []int{0}
		counts := []int{0, 0, 0}

		var callbackFunc = func(line string) {
			if line == "" {
				fmt.Println(fmt.Sprintf("Finished with a file %d, linecount: %d", index[0], counts[index[0]]))
				index[0] += 1
			} else if line != "" || strings.HasSuffix(line, "\n") {
				counts[index[0]] += 1
			}
		}

		t.Log("Files should have 1, 10000, and 100000 lines")
		client.ProcessLogsFromRedis(0, callbackFunc)
		utils.AssertTrue(t, reflect.DeepEqual(counts, []int{1, 10000, 100000}))

		t.Log("Let's wait for the copy thread to finish cleaning up...")
		t.Log("Done Cleaning out Redis!")

		defer func() {
			client.Conn.FlushDB()
			utils.CleanFile(tempFile1)
			utils.CleanFile(tempFile2)
			utils.CleanFile(outFile)
			_ = os.Remove(tempDirPath)
		}()
	})
}

