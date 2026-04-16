import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class Chapter11 {
    private static final String VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{";
    private static final int PUSH_CHUNK_SIZE = 64;
    private static final String DUMMY = UUID.randomUUID().toString();

    public static void main(String[] args) throws Exception {
        new Chapter11().run();
    }

    // Execute all test methods sequentially.
    public void run() throws Exception {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        System.out.println("Using Redis DB 15. Flushing DB 15...");
        conn.flushDB();

        testScriptLoad(conn);
        testCreateStatus(conn);
        testDistributedLockingLua(conn);
        testCountingSemaphoreLua(conn);
        testAutocompleteLua(conn);
        testPurchaseItemLua(conn);
        testShardedListPushPop(conn);
        testShardedBlockingPop(conn);

        System.out.println("\nALL Chapter 11 tests passed.");
    }

    // Test the script loading mechanism.
    public void testScriptLoad(Jedis conn) {
        System.out.println("\n----- testScriptLoad -----");
        Chapter11.ScriptFn fn = Chapter11.scriptLoad("return 1");

        Object r1 = fn.call(conn);
        System.out.println("First call result: " + r1);
        assert "1".equals(String.valueOf(r1));

        conn.scriptFlush();
        Object r2 = fn.call(conn);
        System.out.println("After SCRIPT FLUSH, call result: " + r2);
        assert "1".equals(String.valueOf(r2));

        Object r3 = fn.call(conn, Collections.<String>emptyList(), Collections.<String>emptyList(), true);
        System.out.println("force_eval call result: " + r3);
        assert "1".equals(String.valueOf(r3));

        System.out.println("scriptLoad() OK.");
    }

    // Test creating a status message with Lua.
    public void testCreateStatus(Jedis conn) {
        System.out.println("\n----- testCreateStatus -----");

        conn.del("user:100", "status:id:");
        Long noUser = Chapter11.createStatus(conn, "100", "hello");
        System.out.println("createStatus without user login => " + noUser);
        assert noUser == null;

        conn.hset("user:100", "login", "tom");
        conn.hset("user:100", "posts", "0");

        Map<String, String> extra = new HashMap<String, String>();
        extra.put("extra_field", "extra_value");

        Long sid = Chapter11.createStatus(conn, "100", "hello redis lua", extra);
        System.out.println("created status id => " + sid);
        assert sid != null && sid > 0;

        String skey = "status:" + sid;
        Map<String, String> status = conn.hgetAll(skey);
        System.out.println("status hash => " + status);

        assert "tom".equals(status.get("login"));
        assert String.valueOf(sid).equals(status.get("id"));
        assert "hello redis lua".equals(status.get("message"));
        assert "100".equals(status.get("uid"));
        assert status.get("posted") != null;
        assert "extra_value".equals(status.get("extra_field"));

        String posts = conn.hget("user:100", "posts");
        System.out.println("user posts => " + posts);
        assert "1".equals(posts);

        conn.del("user:100", "status:id:", skey);
        System.out.println("createStatus() OK.");
    }

    // Test distributed lock using Lua.
    public void testDistributedLockingLua(Jedis conn) throws InterruptedException {
        System.out.println("\n----- testDistributedLockingLua -----");

        conn.del("lock:testlock");

        System.out.println("Acquiring lock first time...");
        String id1 = Chapter11.acquireLockWithTimeout(conn, "testlock", 2, 2);
        assert id1 != null;
        System.out.println("Got lock id: " + id1);

        System.out.println("Trying to acquire again without releasing (should fail quickly)...");
        String id2 = Chapter11.acquireLockWithTimeout(conn, "testlock", 0.2, 2);
        assert id2 == null;
        System.out.println("Failed to acquire as expected.");

        System.out.println("Releasing with wrong identifier (should fail)...");
        assert !Chapter11.releaseLock(conn, "testlock", "wrong-id");

        System.out.println("Releasing with correct identifier (should succeed)...");
        assert Chapter11.releaseLock(conn, "testlock", id1);

        System.out.println("Acquiring again (should succeed)...");
        String id3 = Chapter11.acquireLockWithTimeout(conn, "testlock", 2, 1);
        assert id3 != null;
        System.out.println("Got lock id: " + id3);

        System.out.println("Waiting lock to expire...");
        Thread.sleep(1200);

        System.out.println("After expiry, acquiring should succeed...");
        String id4 = Chapter11.acquireLockWithTimeout(conn, "testlock", 2, 1);
        assert id4 != null;
        System.out.println("Got lock id: " + id4);

        conn.del("lock:testlock");
        System.out.println("Lua lock OK.");
    }

    // Test counting semaphore using Lua.
    public void testCountingSemaphoreLua(Jedis conn) throws InterruptedException {
        System.out.println("\n----- testCountingSemaphoreLua -----");

        conn.del("testsem");

        System.out.println("Acquire 3 semaphores (limit=3)...");
        String a = Chapter11.acquireSemaphore(conn, "testsem", 3, 1.0);
        String b = Chapter11.acquireSemaphore(conn, "testsem", 3, 1.0);
        String c = Chapter11.acquireSemaphore(conn, "testsem", 3, 1.0);
        assert a != null && b != null && c != null;
        System.out.println("Got: " + a + ", " + b + ", " + c);

        System.out.println("Acquire 4th (should fail)...");
        String d = Chapter11.acquireSemaphore(conn, "testsem", 3, 1.0);
        assert d == null;
        System.out.println("Failed as expected.");

        System.out.println("Refreshing one token (should succeed)...");
        assert Chapter11.refreshSemaphore(conn, "testsem", a);

        System.out.println("Wait for timeout > 1s, old tokens should expire...");
        Thread.sleep(1200);

        System.out.println("Try acquire again after timeout...");
        String e = Chapter11.acquireSemaphore(conn, "testsem", 3, 1.0);
        assert e != null;
        System.out.println("Got new token: " + e);

        conn.del("testsem");
        System.out.println("Lua semaphore OK.");
    }

    // Test autocomplete using Lua.
    public void testAutocompleteLua(Jedis conn) {
        System.out.println("\n----- testAutocompleteLua -----");

        conn.del("members:g1");

        System.out.println("Prefix range for 'abc' => " + Arrays.toString(Chapter11.findPrefixRange("abc")));

        System.out.println("Adding members...");
        conn.zadd("members:g1", 0, "jeff");
        conn.zadd("members:g1", 0, "jenny");
        conn.zadd("members:g1", 0, "jack");
        conn.zadd("members:g1", 0, "jennifer");
        conn.zadd("members:g1", 0, "john");

        System.out.println("Autocomplete 'je' (expect: jeff/jenny/jennifer)...");
        List<String> r = Chapter11.autocompleteOnPrefix(conn, "g1", "je");
        System.out.println("Result => " + r);
        assert r.contains("jeff");
        assert r.contains("jenny");
        assert r.contains("jennifer");
        assert r.size() >= 3;

        System.out.println("Autocomplete 'ja' (expect: jack)...");
        List<String> r2 = Chapter11.autocompleteOnPrefix(conn, "g1", "ja");
        System.out.println("Result => " + r2);
        assert r2.contains("jack");

        conn.del("members:g1");
        System.out.println("Lua autocomplete OK.");
    }

    // Test purchase item using Lua.
    public void testPurchaseItemLua(Jedis conn) {
        System.out.println("\n----- testPurchaseItemLua -----");

        conn.del("market:", "user:buyer", "user:seller", "inventory:buyer");

        conn.hset("user:buyer", "funds", "5");
        conn.hset("user:seller", "funds", "0");

        conn.zadd("market:", 10, "itemX.seller");

        System.out.println("Buyer funds=5, price=10 => purchase should fail (nil)...");
        boolean ok1 = Chapter11.purchaseItem(conn, "buyer", "itemX", "seller");
        System.out.println("purchase result => " + ok1);
        assert !ok1;

        System.out.println("Add funds to buyer, set funds=20...");
        conn.hset("user:buyer", "funds", "20");

        System.out.println("Purchase should succeed...");
        boolean ok2 = Chapter11.purchaseItem(conn, "buyer", "itemX", "seller");
        System.out.println("purchase result => " + ok2);
        assert ok2;

        String buyerFunds = conn.hget("user:buyer", "funds");
        String sellerFunds = conn.hget("user:seller", "funds");
        System.out.println("buyer funds => " + buyerFunds + ", seller funds => " + sellerFunds);

        assert "10".equals(buyerFunds);
        assert "10".equals(sellerFunds);

        boolean inInv = conn.sismember("inventory:buyer", "itemX");
        System.out.println("inventory contains itemX => " + inInv);
        assert inInv;

        Double stillInMarket = conn.zscore("market:", "itemX.seller");
        System.out.println("market still has item? => " + stillInMarket);
        assert stillInMarket == null;

        conn.del("market:", "user:buyer", "user:seller", "inventory:buyer");
        System.out.println("Lua purchaseItem OK.");
    }

    // Test sharded list push/pop operations.
    public void testShardedListPushPop(Jedis conn) {
        System.out.println("\n----- testShardedListPushPop -----");

        String key = "sh:list";
        deleteByPrefix(conn, key + ":");

        String originalMax = null;
        try {
            List<String> cfg = conn.configGet("list-max-ziplist-entries");
            if (cfg != null && cfg.size() >= 2) {
                originalMax = cfg.get(1);
                System.out.println("Original list-max-ziplist-entries => " + originalMax);
                conn.configSet("list-max-ziplist-entries", "8");
                System.out.println("Temporarily set list-max-ziplist-entries => 8");
            }
        } catch (Exception e) {
            System.out.println("CONFIG SET not permitted, continue with default config.");
        }

        String[] items = new String[30];
        for (int i = 0; i < items.length; i++) items[i] = String.valueOf(i + 1);

        long pushed = Chapter11.shardedRpush(conn, key, items);
        System.out.println("shardedRpush pushed => " + pushed);
        assert pushed == 30;

        for (int i = 1; i <= 30; i++) {
            String v = Chapter11.shardedLPop(conn, key);
            assert String.valueOf(i).equals(v);
        }
        assert Chapter11.shardedLPop(conn, key) == null;

        List<String> letters = new ArrayList<String>();
        for (char c = 'A'; c <= 'Z'; c++) letters.add(String.valueOf(c));

        long pushed2 = Chapter11.shardedLpush(conn, key, letters.toArray(new String[0]));
        System.out.println("shardedLpush pushed => " + pushed2);
        assert pushed2 == 26;

        for (int i = 0; i < 26; i++) {
            String v = Chapter11.shardedRPop(conn, key);
            assert letters.get(i).equals(v);
        }
        assert Chapter11.shardedRPop(conn, key) == null;

        if (originalMax != null) {
            try {
                conn.configSet("list-max-ziplist-entries", originalMax);
                System.out.println("Restored list-max-ziplist-entries => " + originalMax);
            } catch (Exception ignore) {
            }
        }

        deleteByPrefix(conn, key + ":");
        System.out.println("Sharded list push/pop OK.");
    }

    // Test blocking pop on sharded lists.
    public void testShardedBlockingPop(final Jedis conn) throws Exception {
        System.out.println("\n----- testShardedBlockingPop -----");

        final String key = "sh:block";
        deleteByPrefix(conn, key + ":");

        Thread producer1 = new Thread(new Runnable() {
            public void run() {
                Jedis c = null;
                try {
                    Thread.sleep(500);
                    c = new Jedis("localhost");
                    c.select(15);
                    Chapter11.shardedRpush(c, key, "hello");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (c != null) {
                        try {
                            c.disconnect();
                        } catch (Exception ignore) {
                        }
                    }
                }
            }
        });
        producer1.start();

        System.out.println("Calling shardedBlpop(timeout=3)...");
        String v1 = Chapter11.shardedBlpop(conn, key, 3);
        System.out.println("shardedBlpop got => " + v1);
        assert "hello".equals(v1);

        Thread producer2 = new Thread(new Runnable() {
            public void run() {
                Jedis c = null;
                try {
                    Thread.sleep(500);
                    c = new Jedis("localhost");
                    c.select(15);
                    Chapter11.shardedLpush(c, key, "world");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (c != null) {
                        try {
                            c.disconnect();
                        } catch (Exception ignore) {
                        }
                    }
                }
            }
        });
        producer2.start();

        System.out.println("Calling shardedBrpop(timeout=3)...");
        String v2 = Chapter11.shardedBrpop(conn, key, 3);
        System.out.println("shardedBrpop got => " + v2);
        assert "world".equals(v2);

        producer1.join();
        producer2.join();

        assert Chapter11.shardedLPop(conn, key) == null;
        assert Chapter11.shardedRPop(conn, key) == null;

        deleteByPrefix(conn, key + ":");
        System.out.println("Sharded list blocking pop OK.");
    }

    // Delete all keys with a given prefix.
    private void deleteByPrefix(Jedis conn, String prefix) {
        Set<String> keys = conn.keys(prefix + "*");
        if (keys != null && !keys.isEmpty()) {
            conn.del(keys.toArray(new String[0]));
        }
    }

    // Return a function that loads and executes a Lua script.
    public static ScriptFn scriptLoad(String script) {
        AtomicReference<String> sha = new AtomicReference<String>(null);
        return new ScriptFn() {
            @Override
            public Object call(Jedis conn, List<String> keys, List<String> args, boolean force_eval) {
                if (keys == null) {
                    keys = Collections.emptyList();
                }
                if (args == null) {
                    args = Collections.emptyList();
                }
                if (force_eval) {
                    return conn.eval(script, keys, args);
                }
                if (sha.get() == null) {
                    String loadedSha = conn.scriptLoad(script);
                    sha.set(loadedSha);
                }
                try {
                    return conn.evalsha(sha.get(), keys, args);
                } catch (JedisDataException e) {
                    String msg = e.getMessage();
                    if (msg == null || !msg.startsWith("NOSCRIPT")) {
                        throw e;
                    }
                    return conn.eval(script, keys, args);
                }
            }

            @Override
            public Object call(Jedis conn, List<String> keys, List<String> args) {
                return call(conn, keys, args, false);
            }

            @Override
            public Object call(Jedis conn) {
                return call(conn, Collections.emptyList(), Collections.emptyList(), false);
            }
        };
    }

    // Interface for script-calling functions.
    public interface ScriptFn {
        Object call(Jedis conn, List<String> keys, List<String> args, boolean force_eval);
        Object call(Jedis conn, List<String> keys, List<String> args);
        Object call(Jedis conn);
    }

    // Lua script for creating a status message.
    private static final ScriptFn createStatusLua = scriptLoad(
            "local login = redis.call('hget', KEYS[1], 'login')\n" +
                    "if not login then\n" +
                    "    return false\n" +
                    "end\n" +
                    "local id = redis.call('incr', KEYS[2])\n" +
                    "local key = string.format('status:%s', id)\n" +
                    "redis.call('hmset', key,\n" +
                    "    'login', login,\n" +
                    "    'id', id,\n" +
                    "    unpack(ARGV)\n" +
                    ")\n" +
                    "redis.call('hincrby', KEYS[1], 'posts', 1)\n" +
                    "return id\n"
    );

    // Create a status message using Lua script.
    public static Long createStatus(Jedis conn, String uid, String message, Map<String, String> data) {
        List<String> args = new ArrayList<String>();
        args.add("message");
        args.add(message);
        long posted = System.currentTimeMillis() / 1000;
        args.add("posted");
        args.add(String.valueOf(posted));
        args.add("uid");
        args.add(uid);
        if (data != null) {
            for (Map.Entry<String, String> entry : data.entrySet()) {
                args.add(entry.getKey());
                args.add(entry.getValue());
            }
        }
        List<String> keys = Arrays.asList("user:" + uid, "status:id:");
        Object result = createStatusLua.call(conn, keys, args);
        if (result == null) {
            return null;
        }
        if (result instanceof Long) {
            return (Long) result;
        }
        return Long.parseLong(String.valueOf(result));
    }

    // Create a status message with only required fields.
    public static Long createStatus(Jedis conn, String uid, String message) {
        return createStatus(conn, uid, message, null);
    }

    // Lua script for acquiring a lock with timeout.
    private static final ScriptFn acquireLockWithTimeoutLua = scriptLoad(
            "if redis.call('exists',KEYS[1]) == 0 then\n" +
                    "   return redis.call('setex',KEYS[1],unpack(ARGV))\n" +
                    "end\n" +
                    "return nil\n"
    );

    // Acquire a distributed lock using Lua script.
    public static String acquireLockWithTimeout(Jedis conn, String lockname, double acquireTimeout, double lockTimeout) {
        String identifier = UUID.randomUUID().toString();
        String lockKey = "lock:" + lockname;
        int lockTimeoutCeil = (int) Math.ceil(lockTimeout);
        boolean acquired = false;
        long end = System.currentTimeMillis() + (long) (acquireTimeout * 1000);
        List<String> keys = Collections.singletonList(lockKey);
        while (System.currentTimeMillis() < end && !acquired) {
            List<String> args = Arrays.asList(String.valueOf(lockTimeoutCeil), identifier);
            Object response = acquireLockWithTimeoutLua.call(conn, keys, args);
            acquired = (response != null) && "OK".equals(String.valueOf(response));
            if (!acquired) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return acquired ? identifier : null;
    }

    // Acquire a lock with default timeouts.
    public static String acquireLockWithTimeout(Jedis conn, String lockname) {
        return acquireLockWithTimeout(conn, lockname, 10, 10);
    }

    // Lua script for releasing a lock.
    private static final ScriptFn releaseLockLua = scriptLoad(
            "if redis.call('get',KEYS[1]) == ARGV[1] then\n" +
                    "   return redis.call('del',KEYS[1])\n" +
                    "end\n" +
                    "return false\n"
    );

    // Release a distributed lock using Lua script.
    public static boolean releaseLock(Jedis conn, String lockname, String identifier) {
        String lockKey = "lock:" + lockname;
        Object response = releaseLockLua.call(conn,
                Collections.singletonList(lockKey),
                Collections.singletonList(identifier)
        );
        if (response == null) {
            return false;
        }
        if (response instanceof Boolean) {
            return (Boolean) response;
        }
        String str = String.valueOf(response);
        if ("false".equalsIgnoreCase(str) || "nil".equalsIgnoreCase(str)) {
            return false;
        }
        try {
            return Long.parseLong(str) > 0;
        } catch (NumberFormatException e) {
            return true;
        }
    }

    // Lua script for acquiring a counting semaphore.
    private static final ScriptFn acquireSemaphoreLua = scriptLoad(
            "redis.call('zremrangebyscore',KEYS[1],'-inf',ARGV[1])\n" +
                    "if redis.call('zcard',KEYS[1]) < tonumber(ARGV[2]) then\n" +
                    "   redis.call('zadd',KEYS[1],ARGV[3],ARGV[4])\n" +
                    "   return ARGV[4]\n" +
                    "end\n" +
                    "return nil\n"
    );

    // Acquire a counting semaphore using Lua script.
    public static String acquireSemaphore(Jedis conn, String semname, long limit, double timeout) {
        double now = System.currentTimeMillis() / 1000.0;
        String identifier = UUID.randomUUID().toString();
        List<String> keys = Collections.singletonList(semname);
        List<String> args = Arrays.asList(
                String.valueOf(now - timeout),
                String.valueOf(limit),
                String.valueOf(now),
                identifier
        );
        Object response = acquireSemaphoreLua.call(conn, keys, args);
        return (response == null) ? null : String.valueOf(response);
    }

    // Acquire a semaphore with default timeout.
    public static String acquireSemaphore(Jedis conn, String semname, long limit) {
        return acquireSemaphore(conn, semname, limit, 10);
    }

    // Lua script for refreshing a semaphore.
    private static final ScriptFn refreshSemaphoreLua = scriptLoad(
            "if redis.call('zscore',KEYS[1],ARGV[1]) then\n" +
                    "   return redis.call('zadd',KEYS[1],ARGV[2],ARGV[1]) or true\n" +
                    "end\n" +
                    "return nil\n"
    );

    // Refresh a semaphore's timestamp.
    public static boolean refreshSemaphore(Jedis conn, String semname, String identifier) {
        double now = System.currentTimeMillis() / 1000.0;
        Object response = refreshSemaphoreLua.call(
                conn,
                Collections.singletonList(semname),
                Arrays.asList(identifier, String.valueOf(now))
        );
        return response != null;
    }

    // Lua script for autocomplete on prefix.
    private static final ScriptFn autocompleteOnPrefixLua = scriptLoad(
            "redis.call('zadd',KEYS[1],0,ARGV[1],0,ARGV[2])\n" +
                    "local sindex = redis.call('zrank',KEYS[1],ARGV[1])\n" +
                    "local eindex = redis.call('zrank',KEYS[1],ARGV[2])\n" +
                    "local orange = math.min(sindex + 9, eindex - 2)\n" +
                    "redis.call('zrem',KEYS[1],unpack(ARGV))\n" +
                    "return redis.call('zrange',KEYS[1],sindex,orange)\n"
    );

    // Get autocomplete suggestions for a prefix.
    public static List<String> autocompleteOnPrefix(Jedis conn, String guild, String prefix) {
        String[] range = findPrefixRange(prefix);
        String start = range[0];
        String end = range[1];

        String identifier = UUID.randomUUID().toString();
        List<String> keys = Collections.singletonList("members:" + guild);
        List<String> args = Arrays.asList(start + identifier, end + identifier);
        Object response = autocompleteOnPrefixLua.call(conn, keys, args);
        List<String> items = castStringList(response);

        List<String> out = new ArrayList<String>(items.size());
        for (String item : items) {
            if (item != null && item.indexOf('{') < 0) {
                out.add(item);
            }
        }
        return out;
    }

    // Find the lexicographic range for a given prefix.
    public static String[] findPrefixRange(String prefix) {
        if (prefix == null || prefix.trim().length() == 0) {
            throw new IllegalArgumentException("prefix must not be empty");
        }
        prefix = prefix.toLowerCase(Locale.ROOT);
        char last = prefix.charAt(prefix.length() - 1);
        int posn = VALID_CHARACTERS.indexOf(last);
        if (posn < 0) {
            throw new IllegalArgumentException("invalid character in prefix: " + last);
        }
        char suffix = VALID_CHARACTERS.charAt(posn > 0 ? posn - 1 : 0);
        String start = prefix.substring(0, prefix.length() - 1) + suffix + "{";
        String end = prefix + "{";
        return new String[]{start, end};
    }

    // Safely cast a script response to a list of strings.
    private static List<String> castStringList(Object response) {
        if (response == null) return Collections.emptyList();
        if (response instanceof List<?>) {
            List<?> raw = (List<?>) response;
            List<String> out = new ArrayList<String>(raw.size());
            for (Object o : raw) {
                out.add(o == null ? null : String.valueOf(o));
            }
            return out;
        }
        return Collections.singletonList(response.toString());
    }

    // Lua script for purchasing an item from market.
    private static final ScriptFn purchaseItemLua = scriptLoad(
            "local price = tonumber(redis.call('zscore',KEYS[1],ARGV[1]))\n" +
                    "if not price then\n" +
                    "   return 0\n" +
                    "end\n" +
                    "local funds = tonumber(redis.call('hget',KEYS[2],'funds'))\n" +
                    "if (not funds) or (funds < price) then\n" +
                    "   return nil\n" +
                    "end\n" +
                    "redis.call('hincrby',KEYS[3],'funds',price)\n" +
                    "redis.call('hincrby',KEYS[2],'funds',-price)\n" +
                    "redis.call('sadd',KEYS[4],ARGV[2])\n" +
                    "redis.call('zrem',KEYS[1],ARGV[1])\n" +
                    "return true\n"
    );

    // Purchase an item using Lua script.
    public static boolean purchaseItem(Jedis conn, String buyerId, String itemId, String sellerId) {
        String buyer = "user:" + buyerId;
        String seller = "user:" + sellerId;
        String item = itemId + "." + sellerId;
        String inventory = "inventory:" + buyerId;

        List<String> keys = Arrays.asList("market:", buyer, seller, inventory);
        List<String> args = Arrays.asList(item, itemId);

        Object response = purchaseItemLua.call(conn, keys, args);
        return response != null;
    }

    // Lua script for pushing to a sharded list.
    private static final ScriptFn shardedPushLua = scriptLoad(
            "local max = tonumber(redis.call('config','get','list-max-ziplist-entries')[2])\n" +
                    "if #ARGV < 2 or max < 2 then return 0 end\n" +
                    "local skey = ARGV[1] == 'lpush' and KEYS[2] or KEYS[3]\n" +
                    "local shard = redis.call('get', skey) or '0'\n" +
                    "while 1 do\n" +
                    "  local current = tonumber(redis.call('llen', KEYS[1] .. shard))\n" +
                    "  local topush = math.min(#ARGV - 1, max - current - 1)\n" +
                    "  if topush > 0 then\n" +
                    "    redis.call(ARGV[1], KEYS[1] .. shard, unpack(ARGV, 2, topush + 1))\n" +
                    "    return topush\n" +
                    "  end\n" +
                    "  shard = redis.call(ARGV[1] == 'lpush' and 'decr' or 'incr', skey)\n" +
                    "end\n"
    );

    // Helper to push items onto a sharded list.
    public static long shardedPushHelper(Jedis conn, String key, String cmd, String... items) {
        List<String> itemList = new ArrayList<String>();
        if (items != null) {
            Collections.addAll(itemList, items);
        }
        long total = 0;
        List<String> keys = Arrays.asList(key + ":", key + ":first", key + ":last");
        while (!itemList.isEmpty()) {
            int end = Math.min(PUSH_CHUNK_SIZE, itemList.size());
            List<String> args = new ArrayList<String>(end + 1);
            args.add(cmd);
            args.addAll(itemList.subList(0, end));
            Object pushed = shardedPushLua.call(conn, keys, args);
            total += (Long) pushed;
            itemList.subList(0, end).clear();
        }
        return total;
    }

    // Left push onto a sharded list.
    public static long shardedLpush(Jedis conn, String key, String... items) {
        return shardedPushHelper(conn, key, "lpush", items);
    }

    // Right push onto a sharded list.
    public static long shardedRpush(Jedis conn, String key, String... items) {
        return shardedPushHelper(conn, key, "rpush", items);
    }

    // Lua script for popping from a sharded list.
    private static final ScriptFn shardedListPopLua = scriptLoad(
            "local skey = ARGV[1] == 'lpop' and KEYS[2] or KEYS[3]\n" +
                    "local shard = redis.call('get', skey) or '0'\n" +
                    "local ret = redis.call(ARGV[1], KEYS[1] .. shard)\n" +
                    "if not ret or redis.call('llen', KEYS[1] .. shard) == 0 then\n" +
                    "    local oshard = redis.call('get', skey) or '0'\n" +
                    "    if shard == oshard then\n" +
                    "        return ret\n" +
                    "    end\n" +
                    "end\n" +
                    "local cmd = ARGV[1] == 'lpop' and 'decr' or 'incr'\n" +
                    "shard = redis.call(cmd, skey)\n" +
                    "if not ret then\n" +
                    "    ret = redis.call(ARGV[1], KEYS[1] .. shard)\n" +
                    "end\n" +
                    "return ret"
    );

    // Left pop from a sharded list.
    public static String shardedLPop(Jedis conn, String key) {
        List<String> keys = Arrays.asList(key + ":", key + ":first", key + ":last");
        List<String> args = Collections.singletonList("lpop");
        Object response = shardedListPopLua.call(conn, keys, args);
        return (response == null) ? null : String.valueOf(response);
    }

    // Right pop from a sharded list.
    public static String shardedRPop(Jedis conn, String key) {
        List<String> keys = Arrays.asList(key + ":", key + ":first", key + ":last");
        List<String> args = Collections.singletonList("rpop");
        Object response = shardedListPopLua.call(conn, keys, args);
        return (response == null) ? null : String.valueOf(response);
    }

    // Lua script for assisting blocking pop on sharded lists.
    private static final ScriptFn shardedBpopHelperLua = scriptLoad(
            "local shard = redis.call('get', KEYS[2]) or '0'\n" +
                    "if shard ~= ARGV[1] then\n" +
                    "    redis.call(ARGV[2], KEYS[1], ARGV[3])\n" +
                    "end\n"
    );

    // Helper for blocking pop from a sharded list.
    public static String shardedBpopHelper(Jedis conn, String key, int timeout, boolean isLeft) {
        int t = Math.max(timeout, 0);
        if (t == 0) t = 2 * 64;
        long end = System.currentTimeMillis() + t * 1000L;

        String endp = isLeft ? ":first" : ":last";
        String pushCmd = isLeft ? "lpush" : "rpush";

        while (System.currentTimeMillis() < end) {
            String result = isLeft ? shardedLPop(conn, key) : shardedRPop(conn, key);
            if (result != null && !DUMMY.equals(result)) {
                return result;
            }

            String shard = conn.get(key + endp);
            if (shard == null) shard = "0";
            String shardListKey = key + ":" + shard;

            shardedBpopHelperLua.call(conn,
                    Arrays.asList(shardListKey, key + endp),
                    Arrays.asList(shard, pushCmd, DUMMY),
                    true);

            List<String> r = isLeft ? conn.blpop(1, shardListKey) : conn.brpop(1, shardListKey);
            if (r != null && r.size() == 2) {
                String val = r.get(1);
                if (!DUMMY.equals(val)) {
                    return val;
                }
            }
        }
        return null;
    }

    // Blocking left pop from a sharded list.
    public static String shardedBlpop(Jedis conn, String key, int timeout) {
        return shardedBpopHelper(conn, key, timeout, true);
    }

    // Blocking right pop from a sharded list.
    public static String shardedBrpop(Jedis conn, String key, int timeout) {
        return shardedBpopHelper(conn, key, timeout, false);
    }
}
