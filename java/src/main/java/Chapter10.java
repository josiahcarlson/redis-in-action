import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.javatuples.Pair;
import redis.clients.jedis.*;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

public class Chapter10 {

    private static final ConcurrentHashMap<String, Map<String, Object>> CONFIGS =
            new ConcurrentHashMap<String, Map<String, Object>>();
    private static final ConcurrentHashMap<String, JedisPool> REDIS_CONNECTIONS =
            new ConcurrentHashMap<String, JedisPool>();
    private static final ConcurrentHashMap<String, Long> CHECKED_MS =
            new ConcurrentHashMap<String, Long>();
    private static volatile JedisPool configConnection;

    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

    private static final ConcurrentHashMap<String, Long> EXPECTED =
            new ConcurrentHashMap<String, Long>();

    private static final int SHARD_SIZE = 512;
    private static final int DELAY_EXPECTED = 1000000;

    private static final SimpleDateFormat ISO_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00");

    private static final int HOME_TIMELINE_SIZE = 1000;
    private static final int POSTS_PER_PASS = 1000;

    private static final int SHARDED_TIMELINES_SHARDS = 8;
    private static final int SHARDED_FOLLOWERS_SHARDS = 16;

    static {
        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private static final Pattern QUERY_RE = Pattern.compile("[+-]?[a-z']{2,}");

    private static final Set<String> STOP_WORDS = new HashSet<String>();

    static {
        Collections.addAll(STOP_WORDS, ("able about across after all almost also am among " +
                "an and any are as at be because been but by can " +
                "cannot could dear did do does either else ever " +
                "every for from get got had has have he her hers " +
                "him his how however if in into is it its just " +
                "least let like likely may me might most must my " +
                "neither no nor not of off often on only or other " +
                "our own rather said say says she should since so " +
                "some than that the their them then there these " +
                "they this tis to too twas us wants was we were " +
                "what when where which while who whom why will " +
                "with would yet you your").split(" "));
    }


    public static void main(String[] args) throws Exception {
        new Chapter10().run();
    }

    public void run() throws Exception {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        conn.flushDB();

        seedRedisConfigs(conn);

        JedisPool configPool = new JedisPool(new JedisPoolConfig(), "127.0.0.1", 6379, 2000, null, 15);
        Chapter10.setConfigConnection(configPool);

        Chapter10 ch10 = new Chapter10();

        testCountVisit(conn);
        testSearchAndSort(conn, ch10);
        testFollowUser(conn, ch10);
        testDelayedTasks(conn, ch10);

        System.out.println("\nALL TESTS DONE.");
    }

    private void seedRedisConfigs(Jedis conn) {
        System.out.println("\n----- seedRedisConfigs -----");

        String json = "{\"host\":\"127.0.0.1\",\"port\":6379,\"db\":15,\"timeoutMillis\":2000}";

        conn.set("config:redis:default", json);
        conn.set("config:redis:unique", json);

        for (int i = 0; i < 16; i++) {
            conn.set("config:redis:unique:" + i, json);
        }

        for (int i = 0; i < 8; i++) {
            conn.set("config:redis:timelines:" + i, json);
        }

        for (int i = 0; i < 16; i++) {
            conn.set("config:redis:followers:" + i, json);
        }

        for (int i = 0; i < 16; i++) {
            conn.set("config:redis:list:out:" + i, json);
        }

        System.out.println("seed done.");
    }

    public void testCountVisit(Jedis conn) {
        System.out.println("\n----- testCountVisit -----");

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00");
        fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        String key = "unique:" + fmt.format(new Date());

        conn.del(key);

        String sessionId = UUID.randomUUID().toString();
        Chapter10.countVisit(sessionId);

        String v = conn.get(key);
        System.out.println("unique key: " + key + " => " + v);
        assert v != null && Long.parseLong(v) >= 1L;
    }

    public void testSearchAndSort(Jedis conn, Chapter10 ch10) {
        System.out.println("\n----- testSearchAndSort -----");

        conn.del("idx:foo");
        conn.del("kb:doc:1", "kb:doc:2", "kb:doc:3");

        conn.hset("kb:doc:1", "updated", "100");
        conn.hset("kb:doc:1", "title", "b-title");

        conn.hset("kb:doc:2", "updated", "300");
        conn.hset("kb:doc:2", "title", "a-title");

        conn.hset("kb:doc:3", "updated", "200");
        conn.hset("kb:doc:3", "title", "c-title");

        conn.sadd("idx:foo", "1", "2", "3");

        Chapter10.SearchResult r1 = ch10.searchAndSort(conn, "foo", "updated");
        System.out.println("sort=updated => " + r1.docids);
        assert r1.docids.size() == 3;
        assert "1".equals(r1.docids.get(0));
        assert "3".equals(r1.docids.get(1));
        assert "2".equals(r1.docids.get(2));

        Chapter10.SearchResult r2 = ch10.searchAndSort(conn, "foo", "title");
        System.out.println("sort=title => " + r2.docids);
        assert r2.docids.size() == 3;
        assert "2".equals(r2.docids.get(0));
        assert "1".equals(r2.docids.get(1));
        assert "3".equals(r2.docids.get(2));

        Chapter10.SearchResult r3 = ch10.searchAndSort(conn, "foo", "-updated");
        System.out.println("sort=-updated => " + r3.docids);
        assert r3.docids.size() == 3;
        assert "2".equals(r3.docids.get(0));
        assert "3".equals(r3.docids.get(1));
        assert "1".equals(r3.docids.get(2));
    }

    public void testFollowUser(Jedis conn, Chapter10 ch10) {
        System.out.println("\n----- testFollowUser -----");

        long uid = 1001L;
        long otherUid = 2002L;

        conn.del("following:" + uid);
        conn.del("followers:" + otherUid);
        conn.del("user:" + uid);
        conn.del("user:" + otherUid);

        String pkey = "profile:" + otherUid;
        String hkey = "home:" + uid;
        conn.del(pkey);
        conn.del(hkey);

        double now = System.currentTimeMillis() / 1000.0;
        conn.zadd(pkey, now - 10, "status:1");
        conn.zadd(pkey, now - 5, "status:2");

        boolean ok = ch10.followUser(conn, uid, otherUid);
        System.out.println("followUser => " + ok);
        assert ok;

        Set<Tuple> home = conn.zrevrangeWithScores(hkey, 0, -1);
        System.out.println("home timeline => " + tuplesToElements(home));
        assert home != null && home.size() >= 2;

        List<String> elems = tuplesToElements(home);
        assert elems.contains("status:1");
        assert elems.contains("status:2");
    }

    public void testDelayedTasks(Jedis conn, Chapter10 ch10) throws Exception {
        System.out.println("\n----- testDelayedTasks -----");

        conn.del("delayed:");
        conn.del("queue:default");

        Chapter10.PollQueueThread t = ch10.new PollQueueThread();
        t.start();

        List<String> args = new ArrayList<String>();
        args.add("hello");
        String id = ch10.executeLater(conn, "default", "test_task", args, 200);
        System.out.println("scheduled id => " + id);

        Thread.sleep(800);

        long qlen = conn.llen("queue:default");
        System.out.println("queue:default len => " + qlen);
        assert qlen >= 1;

        t.quitThread();
        t.join(1000);
    }

    private List<String> tuplesToElements(Set<Tuple> tuples) {
        List<String> out = new ArrayList<String>();
        if (tuples == null) return out;
        for (Tuple t : tuples) {
            out.add(t.getElement());
        }
        return out;
    }


    private static void returnJedis(JedisPool pool, Jedis jedis) {
        if (pool != null && jedis != null) {
            pool.returnResource(jedis);
        }
    }

    private static void returnBrokenJedis(JedisPool pool, Jedis jedis) {
        if (pool != null && jedis != null) {
            pool.returnBrokenResource(jedis);
        }
    }

    private static boolean eq(Object a, Object b) {
        return a == b || (a != null && a.equals(b));
    }

    private static long getOrDefaultLong(Map<String, Long> map, String key, long def) {
        Long v = map.get(key);
        return v != null ? v.longValue() : def;
    }

    public static void setConfigConnection(JedisPool configConn) {
        configConnection = configConn;
    }

    public static Map<String, Object> getConfig(JedisPool conn, String type, String component, int wait) {
        String key = "config:" + type + ":" + component;
        long now = System.currentTimeMillis();
        long waitMillis = Math.max(0, wait) * 1000L;

        long lastChecked = getOrDefaultLong(CHECKED_MS, key, 0L);
        if (lastChecked < now - waitMillis) {
            CHECKED_MS.put(key, Long.valueOf(now));

            String json = null;
            Jedis jedis = null;
            boolean ok = true;
            try {
                jedis = conn.getResource();
                json = jedis.get(key);
            } catch (Exception e) {
                ok = false;
                if (jedis != null) returnBrokenJedis(conn, jedis);
                throw new RuntimeException(e);
            } finally {
                if (ok && jedis != null) returnJedis(conn, jedis);
            }

            if (json == null || json.trim().length() == 0) {
                json = "{}";
            }

            Map<String, Object> config = parseJsonToMap(json);
            Map<String, Object> oldConfig = CONFIGS.get(key);
            if (!eq(config, oldConfig)) {
                CONFIGS.put(key, config);
            }
        }
        return CONFIGS.get(key);
    }

    private static Map<String, Object> parseJsonToMap(String json) {
        Map<String, Object> map = (Map<String, Object>) GSON.fromJson(json, MAP_TYPE);
        return (map == null) ? Collections.<String, Object>emptyMap() : map;
    }

    public static String shardKey(String base, String key, long totalElements, int shardSize) {
        long shardId;
        if (isDigit(key)) {
            shardId = Integer.parseInt(key, 10) / shardSize;
        } else {
            CRC32 crc32 = new CRC32();
            crc32.update(key.getBytes());
            long shards = 2 * totalElements / shardSize;
            shardId = Math.abs(((int) crc32.getValue()) % (int) shards);
        }
        return base + ":" + shardId;
    }

    public static boolean isDigit(String str) {
        if (str == null || str.length() == 0) return false;
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isDigit(str.charAt(i))) return false;
        }
        return true;
    }

    public static JedisPool getRedisConnection(String component, int wait) {
        String key = "config:redis:" + component;
        Map<String, Object> oldConfig = CONFIGS.get(key);
        Map<String, Object> config = getConfig(configConnection, "redis", component, wait);
        if (config == null) config = Collections.<String, Object>emptyMap();

        if (!eq(config, oldConfig)) {
            CONFIGS.put(key, config);
            JedisPool newConn = createJedisPoolFromConfig(config);
            REDIS_CONNECTIONS.put(key, newConn);
        }
        return REDIS_CONNECTIONS.get(key);
    }

    public static JedisPool getShardedConnection(String component, String key, long shardCount, int wait) {
        String shard = shardKey(component, "x" + String.valueOf(key), shardCount, 2);
        return getRedisConnection(shard, wait);
    }

    public interface ShardedFunction<T> {
        T call(JedisPool conn, String key, Object... args);
    }

    public interface ShardedCaller<T> {
        T call(String key, Object... args);
    }

    public static <T> ShardedCaller<T> shardedConnection(
            final String component,
            final long shardCount,
            final int wait,
            final ShardedFunction<T> function) {

        return new ShardedCaller<T>() {
            public T call(String key, Object... args) {
                JedisPool conn = getShardedConnection(component, key, shardCount, wait);
                return function.call(conn, key, args);
            }
        };
    }

    public static long getExpected(Jedis conn, String key, Calendar today) {
        if (!EXPECTED.containsKey(key)) {
            String exkey = key + ":expected";
            String expectedStr = conn.get(exkey);

            long expected;
            if (expectedStr == null) {
                Calendar yesterday = (Calendar) today.clone();
                yesterday.add(Calendar.DATE, -1);

                expectedStr = conn.get("unique:" + ISO_FORMAT.format(yesterday.getTime()));
                expected = (expectedStr != null) ? Long.parseLong(expectedStr) : (long) DELAY_EXPECTED;

                expected = (long) Math.pow(2, (long) (Math.ceil(Math.log(expected * 1.5) / Math.log(2))));
                if (conn.setnx(exkey, String.valueOf(expected)) == 0) {
                    expectedStr = conn.get(exkey);
                    expected = Long.parseLong(expectedStr);
                }
            } else {
                expected = Long.parseLong(expectedStr);
            }
            EXPECTED.put(key, Long.valueOf(expected));
        }
        Long v = EXPECTED.get(key);
        return v != null ? v.longValue() : 0L;
    }

    public static Pair<JedisPool, Long> getExpected(String key, Calendar today) {
        JedisPool pool = getRedisConnection("unique", 1);
        Jedis jedis = null;
        boolean ok = true;
        try {
            jedis = pool.getResource();
            long expected = getExpected(jedis, key, today);
            return Pair.with(pool, Long.valueOf(expected));
        } catch (Exception e) {
            ok = false;
            if (jedis != null) returnBrokenJedis(pool, jedis);
            throw new RuntimeException(e);
        } finally {
            if (ok && jedis != null) returnJedis(pool, jedis);
        }
    }

    public static final ShardedCaller<Void> COUNT_VISIT =
            shardedConnection("unique", 16, 1, new ShardedFunction<Void>() {
                public Void call(JedisPool pool, String sessionId, Object... args) {
                    Calendar today = Calendar.getInstance();
                    String key = "unique:" + ISO_FORMAT.format(today.getTime());

                    Pair<JedisPool, Long> p = getExpected(key, today);
                    JedisPool conn2 = p.getValue0();
                    long expected = p.getValue1().longValue();

                    long id = Long.parseLong(sessionId.replace("-", "").substring(0, 15), 16);

                    Jedis jedis = null;
                    boolean ok = true;
                    boolean added;
                    try {
                        jedis = pool.getResource();
                        added = (shardSadd(jedis, key, id, expected, SHARD_SIZE).longValue() == 1L);
                    } catch (Exception e) {
                        ok = false;
                        if (jedis != null) returnBrokenJedis(pool, jedis);
                        throw new RuntimeException(e);
                    } finally {
                        if (ok && jedis != null) returnJedis(pool, jedis);
                    }

                    if (added) {
                        Jedis jedis2 = null;
                        boolean ok2 = true;
                        try {
                            jedis2 = conn2.getResource();
                            jedis2.incr(key);
                        } catch (Exception e) {
                            ok2 = false;
                            if (jedis2 != null) returnBrokenJedis(conn2, jedis2);
                            throw new RuntimeException(e);
                        } finally {
                            if (ok2 && jedis2 != null) returnJedis(conn2, jedis2);
                        }
                    }
                    return null;
                }
            });

    public static void countVisit(String sessionId) {
        COUNT_VISIT.call(sessionId, new Object[0]);
    }

    public static Long shardSadd(Jedis conn, String base, long member, long totalElements, int shardSize) {
        String shard = shardKey(base, "x" + String.valueOf(member), totalElements, shardSize);
        return conn.sadd(shard, String.valueOf(member));
    }

    public static final class Query {
        public final List<List<String>> all = new ArrayList<List<String>>();
        public final Set<String> unwanted = new HashSet<String>();
    }

    private String setCommon(Transaction trans, String method, int ttl, String... items) {
        String[] keys = new String[items.length];
        for (int i = 0; i < items.length; i++) {
            keys[i] = "idx:" + items[i];
        }

        String id = UUID.randomUUID().toString();
        try {
            trans.getClass()
                    .getDeclaredMethod(method, String.class, String[].class)
                    .invoke(trans, new Object[]{"idx:" + id, keys});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        trans.expire("idx:" + id, ttl);
        return id;
    }

    public String intersect(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sinterstore", ttl, items);
    }

    public String union(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sunionstore", ttl, items);
    }

    public String difference(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sdiffstore", ttl, items);
    }

    public Query parse(String queryStringString) {
        Query queryString = new Query();
        Set<String> current = new HashSet<String>();

        Matcher matcher = QUERY_RE.matcher(queryStringString.toLowerCase());
        while (matcher.find()) {
            String word = matcher.group().trim();
            char prefix = word.charAt(0);

            if (prefix == '+' || prefix == '-') {
                word = word.substring(1);
            }

            if (word.length() < 2 || STOP_WORDS.contains(word)) {
                continue;
            }

            if (prefix == '-') {
                queryString.unwanted.add(word);
                continue;
            }

            if (!current.isEmpty() && prefix != '+') {
                queryString.all.add(new ArrayList<String>(current));
                current.clear();
            }
            current.add(word);
        }

        if (!current.isEmpty()) {
            queryString.all.add(new ArrayList<String>(current));
        }
        return queryString;
    }

    public String parseAndSearch(Jedis conn, String queryStringString, int ttl) {
        Query queryString = parse(queryStringString);
        if (queryString.all.isEmpty()) return null;

        List<String> toIntersect = new ArrayList<String>();
        for (List<String> syn : queryString.all) {
            if (syn.size() > 1) {
                Transaction trans = conn.multi();
                toIntersect.add(union(trans, ttl, syn.toArray(new String[syn.size()])));
                trans.exec();
            } else {
                toIntersect.add(syn.get(0));
            }
        }

        String intersectResult;
        if (toIntersect.size() > 1) {
            Transaction trans = conn.multi();
            intersectResult = intersect(trans, ttl, toIntersect.toArray(new String[toIntersect.size()]));
            trans.exec();
        } else {
            intersectResult = toIntersect.get(0);
        }

        if (!queryString.unwanted.isEmpty()) {
            String[] keys = queryString.unwanted.toArray(new String[queryString.unwanted.size() + 1]);
            keys[keys.length - 1] = intersectResult;
            Transaction trans = conn.multi();
            intersectResult = difference(trans, ttl, keys);
            trans.exec();
        }

        return intersectResult;
    }

    public SearchResult searchAndSort(Jedis conn, String queryStringString, String sort) {
        return searchAndSort(conn, queryStringString, null, 300, sort, 0, 20);
    }

    public SearchResult searchAndSort(Jedis conn, String queryStringString, String id, int ttl, String sort, int start, int num) {
        boolean desc = sort.startsWith("-");
        if (desc) sort = sort.substring(1);

        boolean alpha = !("updated".equals(sort) || "id".equals(sort) || "created".equals(sort));
        String by = "kb:doc:*->" + sort;

        String rid = (id != null) ? id : parseAndSearch(conn, queryStringString, ttl);
        if (rid == null) {
            return new SearchResult(null, 0L, Collections.<String>emptyList());
        }

        conn.expire("idx:" + rid, ttl);

        Transaction trans = conn.multi();
        trans.scard("idx:" + rid);

        SortingParams params = new SortingParams();
        if (desc) params.desc();
        if (alpha) params.alpha();
        params.by(by);
        params.limit(start, num);

        trans.sort("idx:" + rid, params);

        List<Object> results = trans.exec();
        long count = ((Long) results.get(0)).longValue();

        @SuppressWarnings("unchecked")
        List<String> docids = (List<String>) results.get(1);

        return new SearchResult(rid, count, docids);
    }

    public SearchGetValuesResult searchGetValues(Jedis conn, String queryString, String id, int ttl, String sort, int start, int num) {
        SearchResult searchResult = searchAndSort(conn, queryString, id, ttl, sort, 0, start + num);
        String keyPattern = "kb:doc:%s";
        String sortField = sort.startsWith("-") ? sort.substring(1) : sort;

        Pipeline pipe = conn.pipelined();
        for (String docid : searchResult.docids) {
            pipe.hget(String.format(keyPattern, docid), sortField);
        }

        List<Object> result = pipe.syncAndReturnAll();
        List<Pair<String, String>> dataPairs = new ArrayList<Pair<String, String>>(searchResult.docids.size());

        for (int i = 0; i < searchResult.docids.size(); i++) {
            String docid = searchResult.docids.get(i);
            Object value = (i < result.size()) ? result.get(i) : null;
            String col = (value == null) ? null : String.valueOf(value);
            dataPairs.add(Pair.with(docid, col));
        }

        return new SearchGetValuesResult(searchResult.count, dataPairs, searchResult.id);
    }

    public ShardResults getShardResults(String component, int shards, String queryString, List<String> ids, int ttl, String sort, int start, int num, int wait) {
        long count = 0;
        List<Pair<String, String>> data = new ArrayList<Pair<String, String>>();

        if (ids == null) {
            ids = new ArrayList<String>(shards);
            for (int i = 0; i < shards; i++) ids.add(null);
        }

        for (int shard = 0; shard < shards; shard++) {
            JedisPool pool = getRedisConnection(component + ":" + shard, wait);
            Jedis conn = null;
            boolean ok = true;
            try {
                conn = pool.getResource();
                SearchGetValuesResult result = searchGetValues(conn, queryString, ids.get(shard), ttl, sort, start, num);
                count += result.count;
                data.addAll(result.dataPairs);
                ids.set(shard, result.id);
            } catch (Exception e) {
                ok = false;
                if (conn != null) returnBrokenJedis(pool, conn);
                throw new RuntimeException(e);
            } finally {
                if (ok && conn != null) returnJedis(pool, conn);
            }
        }
        return new ShardResults(count, data, ids);
    }

    private static BigDecimal toNumericKey(Pair<String, String> data) {
        try {
            String s = data.getValue1();
            if (s == null || s.length() == 0) return BigDecimal.ZERO;
            return new BigDecimal(s);
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }

    private static String toStringKey(Pair<String, String> data) {
        String s = data.getValue1();
        return (s == null) ? "" : s;
    }

    public SearchShardsResult searchShards(String component, int shards, String queryString, List<String> ids, int ttl, String sort, int start, int num, int wait) {
        ShardResults shardResults = getShardResults(component, shards, queryString, ids, ttl, sort, start, num, wait);

        final boolean reversed = sort.startsWith("-");
        String sortField = sort.replace("-", "");

        Comparator<Pair<String, String>> comparator;
        if ("updated".equals(sortField) || "id".equals(sortField) || "created".equals(sortField)) {
            comparator = new Comparator<Pair<String, String>>() {
                public int compare(Pair<String, String> o1, Pair<String, String> o2) {
                    return toNumericKey(o1).compareTo(toNumericKey(o2));
                }
            };
        } else {
            comparator = new Comparator<Pair<String, String>>() {
                public int compare(Pair<String, String> o1, Pair<String, String> o2) {
                    return toStringKey(o1).compareTo(toStringKey(o2));
                }
            };
        }

        if (reversed) {
            Collections.sort(shardResults.data, Collections.reverseOrder(comparator));
        } else {
            Collections.sort(shardResults.data, comparator);
        }

        List<String> results = new ArrayList<String>();
        int end = Math.min(shardResults.data.size(), start + num);
        for (int i = start; i < end; i++) {
            results.add(shardResults.data.get(i).getValue0());
        }
        return new SearchShardsResult(shardResults.count, results, shardResults.ids);
    }

    private String zsetCommon(Transaction trans, String method, int ttl, ZParams params, String... sets) {
        String[] keys = new String[sets.length];
        for (int i = 0; i < sets.length; i++) {
            keys[i] = "idx:" + sets[i];
        }

        String id = UUID.randomUUID().toString();
        try {
            trans.getClass()
                    .getDeclaredMethod(method, String.class, ZParams.class, String[].class)
                    .invoke(trans, new Object[]{"idx:" + id, params, keys});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        trans.expire("idx:" + id, ttl);
        return id;
    }

    public String zintersect(Transaction trans, int ttl, ZParams params, String... sets) {
        return zsetCommon(trans, "zinterstore", ttl, params, sets);
    }

    public String zunion(Transaction trans, int ttl, ZParams params, String... sets) {
        return zsetCommon(trans, "zunionstore", ttl, params, sets);
    }

    @SuppressWarnings("unchecked")
    public SearchResult searchAndZsort(Jedis conn, String queryStringString, boolean desc, Map<String, Integer> weights) {
        int ttl = 300;
        int start = 0;
        int num = 20;

        String id = parseAndSearch(conn, queryStringString, ttl);

        int updateWeight = 1;
        Integer uw = weights.get("update");
        if (uw != null) updateWeight = uw.intValue();

        int voteWeight = 0;
        Integer vw = weights.get("vote");
        if (vw != null) voteWeight = vw.intValue();

        String[] keys = new String[]{id, "sort:update", "sort:votes"};
        Transaction trans = conn.multi();
        id = zintersect(trans, ttl, new ZParams().weights(0, updateWeight, voteWeight), keys);

        trans.zcard("idx:" + id);
        if (desc) {
            trans.zrevrange("idx:" + id, start, start + num - 1);
        } else {
            trans.zrange("idx:" + id, start, start + num - 1);
        }

        List<Object> results = trans.exec();

        return new SearchResult(
                id,
                ((Long) results.get(results.size() - 2)).longValue(),
                new ArrayList<String>((Set<String>) results.get(results.size() - 1)));
    }

    public SearchResult searchAndZsort(Jedis conn, String queryStringString, String id, int ttl, int update, int vote, int start, int num, boolean desc) {
        String baseId = (id != null) ? id : parseAndSearch(conn, queryStringString, ttl);
        if (baseId == null) {
            return new SearchResult(null, 0L, Collections.<String>emptyList());
        }

        conn.expire("idx:" + baseId, ttl);

        String[] keys = new String[]{baseId, "sort:update", "sort:votes"};

        Transaction trans = conn.multi();
        String zsetId = zintersect(trans, ttl, new ZParams().weights(0, update, vote), keys);

        trans.zcard("idx:" + zsetId);

        if (desc) {
            trans.zrevrange("idx:" + zsetId, start, start + num - 1);
        } else {
            trans.zrange("idx:" + zsetId, start, start + num - 1);
        }

        List<Object> res = trans.exec();
        long count = ((Long) res.get(res.size() - 2)).longValue();

        @SuppressWarnings("unchecked")
        Set<String> docidSet = (Set<String>) res.get(res.size() - 1);

        return new SearchResult(zsetId, count, new ArrayList<String>(docidSet));
    }

    public SearchZsetValuesResult searchGetZsetValues(Jedis conn, String query, String id, int ttl, int update, int vote, int start, int num, boolean desc) {
        SearchResult sr = searchAndZsort(conn, query, id, ttl, update, vote, 0, 1, desc);

        String zkey = "idx:" + sr.id;

        Set<Tuple> data;
        if (desc) {
            data = conn.zrevrangeWithScores(zkey, 0, start + num - 1);
        } else {
            data = conn.zrangeWithScores(zkey, 0, start + num - 1);
        }

        return new SearchZsetValuesResult(sr.count, data, sr.id);
    }

    public SearchShardsZsetResult searchShardsZset(String component, int shards, String query, List<String> ids, int ttl, int update, int vote, int start, int num, boolean desc, int wait) {
        long count = 0;
        List<Tuple> data = new ArrayList<Tuple>();

        if (ids == null) {
            ids = new ArrayList<String>(shards);
            for (int i = 0; i < shards; i++) ids.add(null);
        }

        for (int shard = 0; shard < shards; shard++) {
            JedisPool pool = getRedisConnection(component + ":" + shard, wait);

            Jedis conn = null;
            boolean ok = true;
            try {
                conn = pool.getResource();
                SearchZsetValuesResult r = searchGetZsetValues(conn, query, ids.get(shard), ttl, update, vote, start, num, desc);
                count += r.count;
                data.addAll(r.data);
                ids.set(shard, r.id);
            } catch (Exception e) {
                ok = false;
                if (conn != null) returnBrokenJedis(pool, conn);
                throw new RuntimeException(e);
            } finally {
                if (ok && conn != null) returnJedis(pool, conn);
            }
        }

        Collections.sort(data, new Comparator<Tuple>() {
            public int compare(Tuple a, Tuple b) {
                double diff = a.getScore() - b.getScore();
                if (diff < 0) return -1;
                if (diff > 0) return 1;
                return 0;
            }
        });
        if (desc) Collections.reverse(data);

        List<String> results = new ArrayList<String>();
        int end = Math.min(data.size(), start + num);
        for (int i = start; i < end; i++) {
            results.add(data.get(i).getElement());
        }

        return new SearchShardsZsetResult(count, results, ids);
    }

    private static final KeyShardedConnection shardedTimelines =
            new KeyShardedConnection("timelines", SHARDED_TIMELINES_SHARDS, 1);

    public boolean followUser(Jedis conn, long uid, long otherUid) {
        String fkey1 = "following:" + uid;
        String fkey2 = "followers:" + otherUid;

        if (conn.zscore(fkey1, String.valueOf(otherUid)) != null) {
            return false;
        }

        long now = System.currentTimeMillis() / 1000;

        Transaction trans = conn.multi();
        trans.zadd(fkey1, now, String.valueOf(otherUid));
        trans.zadd(fkey2, now, String.valueOf(uid));
        trans.zcard(fkey1);
        trans.zcard(fkey2);

        List<Object> response = trans.exec();
        long following = ((Long) response.get(response.size() - 2)).longValue();
        long followers = ((Long) response.get(response.size() - 1)).longValue(); // Java6: no getLast()

        trans = conn.multi();
        trans.hset("user:" + uid, "following", String.valueOf(following));
        trans.hset("user:" + otherUid, "followers", String.valueOf(followers));
        trans.exec();

        String pkey = "profile:" + otherUid;
        JedisPool profilePool = shardedTimelines.get(pkey);

        Jedis pconn = null;
        boolean ok = true;
        Set<Tuple> statusAndScore;
        try {
            pconn = profilePool.getResource();
            statusAndScore = pconn.zrevrangeWithScores(pkey, 0, HOME_TIMELINE_SIZE - 1);
        } catch (Exception e) {
            ok = false;
            if (pconn != null) returnBrokenJedis(profilePool, pconn);
            throw new RuntimeException(e);
        } finally {
            if (ok && pconn != null) returnJedis(profilePool, pconn);
        }

        if (statusAndScore != null && !statusAndScore.isEmpty()) {
            String hkey = "home:" + uid;
            JedisPool homePool = shardedTimelines.get(hkey);

            Jedis hconn = null;
            boolean okh = true;
            try {
                hconn = homePool.getResource();
                Transaction htrans = hconn.multi();
                for (Tuple tuple : statusAndScore) {
                    htrans.zadd(hkey, tuple.getScore(), tuple.getElement());
                }
                htrans.zremrangeByRank(hkey, 0, -HOME_TIMELINE_SIZE - 1);
                htrans.exec();
            } catch (Exception e) {
                okh = false;
                if (hconn != null) returnBrokenJedis(homePool, hconn);
                throw new RuntimeException(e);
            } finally {
                if (okh && hconn != null) returnJedis(homePool, hconn);
            }
        }
        return true;
    }

    private static final KeyDataShardedConnection shardedFollowers =
            new KeyDataShardedConnection("followers", SHARDED_FOLLOWERS_SHARDS, 1);

    public boolean followUser_2(Jedis conn, long uid, long otherUid) {
        String fkey1 = "following:" + uid;
        String fkey2 = "followers:" + otherUid;

        JedisPool sp = shardedFollowers.get(uid, otherUid);
        Jedis sconn = null;
        boolean oks = true;

        long followingAdded;
        long followersAdded;

        try {
            sconn = sp.getResource();
            if (sconn.zscore(fkey1, String.valueOf(otherUid)) != null) {
                return false;
            }
            long now = System.currentTimeMillis() / 1000;

            Transaction trans = sconn.multi();
            trans.zadd(fkey1, now, String.valueOf(otherUid));
            trans.zadd(fkey2, now, String.valueOf(uid));
            List<Object> response = trans.exec();

            followingAdded = ((Long) response.get(0)).longValue();
            followersAdded = ((Long) response.get(1)).longValue();

        } catch (Exception e) {
            oks = false;
            if (sconn != null) returnBrokenJedis(sp, sconn);
            throw new RuntimeException(e);
        } finally {
            if (oks && sconn != null) returnJedis(sp, sconn);
        }

        conn.hincrBy("user:" + uid, "following", followingAdded);
        conn.hincrBy("user:" + otherUid, "followers", followersAdded);

        String pkey = "profile:" + otherUid;

        JedisPool ppool = shardedTimelines.get(pkey);
        Jedis pconn = null;
        boolean okp = true;
        Set<Tuple> statusAndScore;

        try {
            pconn = ppool.getResource();
            statusAndScore = pconn.zrevrangeWithScores(pkey, 0, HOME_TIMELINE_SIZE - 1);
        } catch (Exception e) {
            okp = false;
            if (pconn != null) returnBrokenJedis(ppool, pconn);
            throw new RuntimeException(e);
        } finally {
            if (okp && pconn != null) returnJedis(ppool, pconn);
        }

        if (statusAndScore != null && !statusAndScore.isEmpty()) {
            String hkey = "home:" + uid;
            JedisPool hpool = shardedTimelines.get(hkey);

            Jedis hconn = null;
            boolean okh = true;
            try {
                hconn = hpool.getResource();
                Transaction ht = hconn.multi();
                for (Tuple tuple : statusAndScore) {
                    ht.zadd(hkey, tuple.getScore(), tuple.getElement());
                }
                ht.zremrangeByRank(hkey, 0, -HOME_TIMELINE_SIZE - 1);
                ht.exec();
            } catch (Exception e) {
                okh = false;
                if (hconn != null) returnBrokenJedis(hpool, hconn);
                throw new RuntimeException(e);
            } finally {
                if (okh && hconn != null) returnJedis(hpool, hconn);
            }
        }
        return true;
    }

    public List<Tuple> shardedZrangeByScore(String component, int shards, String key, double min, String max, int num, int wait) {
        List<Tuple> data = new ArrayList<Tuple>();

        for (int shard = 0; shard < shards; shard++) {
            JedisPool pool = getRedisConnection(component + ":" + shard, wait);
            Jedis conn = null;
            boolean ok = true;
            try {
                conn = pool.getResource();
                Set<Tuple> part = conn.zrangeByScoreWithScores(key, String.valueOf(min), max, 0, num);
                if (part != null) data.addAll(part);
            } catch (Exception e) {
                ok = false;
                if (conn != null) returnBrokenJedis(pool, conn);
                throw new RuntimeException(e);
            } finally {
                if (ok && conn != null) returnJedis(pool, conn);
            }
        }

        Collections.sort(data, new Comparator<Tuple>() {
            public int compare(Tuple o1, Tuple o2) {
                double diff = o1.getScore() - o2.getScore();
                if (diff < 0) return -1;
                if (diff > 0) return 1;
                return o1.getElement().compareTo(o2.getElement());
            }
        });

        if (data.size() > num) {
            return new ArrayList<Tuple>(data.subList(0, num));
        }
        return data;
    }

    public void syndicateStatus(long uid, Map<String, Double> post, double start, boolean onLists) {

        String root = "followers";
        String key = "followers:" + uid;
        String base = "home:%s";

        if (onLists) {
            root = "list:out";
            key = "list:out:" + uid;
            base = "list:statuses:%s";
        }

        List<Tuple> followers = shardedZrangeByScore(
                root,
                SHARDED_FOLLOWERS_SHARDS,
                key,
                start,
                "+inf",
                POSTS_PER_PASS,
                1
        );

        Map<String, List<String>> toSend = new HashMap<String, List<String>>();
        double nextStart = start;

        for (Tuple t : followers) {
            String follower = t.getElement();
            nextStart = t.getScore();

            String timeline = String.format(base, follower);
            String shard = shardKey("timelines", timeline, SHARDED_TIMELINES_SHARDS, 2);

            List<String> timelines = toSend.get(shard);
            if (timelines == null) {
                timelines = new ArrayList<String>();
                toSend.put(shard, timelines);
            }
            timelines.add(timeline);
        }

        for (List<String> timelines : toSend.values()) {
            if (timelines == null || timelines.isEmpty()) continue;

            JedisPool pool = shardedTimelines.get(timelines.get(0));
            Jedis j = null;
            boolean ok = true;
            try {
                j = pool.getResource();
                Pipeline pipe = j.pipelined();

                for (String timeline : timelines) {
                    for (Map.Entry<String, Double> e : post.entrySet()) {
                        pipe.zadd(timeline, e.getValue().doubleValue(), e.getKey());
                    }
                    pipe.zremrangeByRank(timeline, 0, -HOME_TIMELINE_SIZE - 1);
                }
                pipe.sync();
            } catch (Exception e) {
                ok = false;
                if (j != null) returnBrokenJedis(pool, j);
                throw new RuntimeException(e);
            } finally {
                if (ok && j != null) returnJedis(pool, j);
            }
        }

        JedisPool dpool = getRedisConnection("default", 1);
        Jedis defaultConn = null;
        boolean okd = true;
        try {
            defaultConn = dpool.getResource();

            Gson gson = new Gson();
            List<String> args = new ArrayList<String>(4);
            args.add(String.valueOf(uid));
            args.add(gson.toJson(post));
            args.add(String.valueOf(nextStart));
            args.add(String.valueOf(onLists));

            if (followers.size() >= POSTS_PER_PASS) {
                executeLater(defaultConn, "default", "syndicate_status", args, 0);
            } else if (!onLists) {
                args.set(2, "0");
                args.set(3, "true");
                executeLater(defaultConn, "default", "syndicate_status", args, 0);
            }
        } catch (Exception e) {
            okd = false;
            if (defaultConn != null) returnBrokenJedis(dpool, defaultConn);
            throw new RuntimeException(e);
        } finally {
            if (okd && defaultConn != null) returnJedis(dpool, defaultConn);
        }
    }

    private static JedisPool createJedisPoolFromConfig(Map<String, Object> config) {
        String host = asString(config.get("host"), "127.0.0.1");
        int port = asInt(config.get("port"), 6379);
        int db = asInt(config.get("db"), 0);
        String password = asString(config.get("password"), null);
        int timeoutMillis = asInt(config.get("timeoutMillis"), 2000);

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        if (password != null && password.trim().length() > 0) {
            return new JedisPool(poolConfig, host, port, timeoutMillis, password, db);
        }
        return new JedisPool(poolConfig, host, port, timeoutMillis, null, db);
    }

    private static String asString(Object value, String def) {
        if (value == null) return def;
        return String.valueOf(value);
    }

    private static int asInt(Object value, int def) {
        if (value == null) return def;
        if (value instanceof Integer) return ((Integer) value).intValue();
        if (value instanceof Long) return ((Long) value).intValue();
        if (value instanceof Double) return ((Double) value).intValue();
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (Exception e) {
            return def;
        }
    }

    public String executeLater(Jedis conn, String queue, String name, List<String> args, long delay) {
        Gson gson = new Gson();
        String identifier = UUID.randomUUID().toString();
        String itemArgs = gson.toJson(args);
        String item = gson.toJson(new String[]{identifier, queue, name, itemArgs});
        if (delay > 0) {
            conn.zadd("delayed:", System.currentTimeMillis() + delay, item);
        } else {
            conn.rpush("queue:" + queue, item);
        }
        return identifier;
    }

    public String acquireLock(Jedis conn, String lockName) {
        return acquireLock(conn, lockName, 10000);
    }

    public String acquireLock(Jedis conn, String lockName, long acquireTimeout) {
        String identifier = UUID.randomUUID().toString();

        long end = System.currentTimeMillis() + acquireTimeout;
        while (System.currentTimeMillis() < end) {
            if (conn.setnx("lock:" + lockName, identifier) == 1) {
                return identifier;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    public boolean releaseLock(Jedis conn, String lockName, String identifier) {
        String lockKey = "lock:" + lockName;

        while (true) {
            conn.watch(lockKey);
            if (identifier.equals(conn.get(lockKey))) {
                Transaction trans = conn.multi();
                trans.del(lockKey);
                List<Object> results = trans.exec();
                if (results == null) {
                    continue;
                }
                return true;
            }
            conn.unwatch();
            break;
        }
        return false;
    }

    public static final class KeyShardedConnection {
        private final String component;
        private final int shards;
        private final int wait;

        public KeyShardedConnection(String component, int shards) {
            this(component, shards, 1);
        }

        public KeyShardedConnection(String component, int shards, int wait) {
            this.component = component;
            this.shards = shards;
            this.wait = wait;
        }

        public JedisPool get(String key) {
            return getShardedConnection(component, key, shards, wait);
        }
    }

    public static final class KeyDataShardedConnection {
        private final String component;
        private final int shards;
        private final int wait;

        public KeyDataShardedConnection(String component, int shards) {
            this(component, shards, 1);
        }

        public KeyDataShardedConnection(String component, int shards, int wait) {
            this.component = component;
            this.shards = shards;
            this.wait = wait;
        }

        public JedisPool get(long id1, long id2) {
            if (id2 < id1) {
                long tmp = id1;
                id1 = id2;
                id2 = tmp;
            }
            String key = id1 + ":" + id2;
            return getShardedConnection(component, key, shards, wait);
        }
    }

    public class PollQueueThread extends Thread {
        private Jedis conn;
        private boolean quit;
        private Gson gson = new Gson();

        public PollQueueThread() {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
        }

        public void quitThread() {
            quit = true;
        }

        public void run() {
            while (!quit) {
                Set<Tuple> items = conn.zrangeWithScores("delayed:", 0, 0);
                Tuple item = (items != null && items.size() > 0) ? items.iterator().next() : null;

                if (item == null || item.getScore() > System.currentTimeMillis()) {
                    try {
                        sleep(10);
                    } catch (InterruptedException ie) {
                        Thread.interrupted();
                    }
                    continue;
                }

                String json = item.getElement();
                String[] values = (String[]) gson.fromJson(json, String[].class);
                String identifier = values[0];
                String queue = values[1];

                String locked = acquireLock(conn, identifier);
                if (locked == null) {
                    continue;
                }

                if (conn.zrem("delayed:", json) == 1) {
                    conn.rpush("queue:" + queue, json);
                }

                releaseLock(conn, identifier, locked);
            }
        }
    }

    public static final class SearchResult {
        public final String id;
        public final long count;
        public final List<String> docids;

        public SearchResult(String id, long count, List<String> docids) {
            this.id = id;
            this.count = count;
            this.docids = docids;
        }
    }

    public static final class SearchGetValuesResult {
        public final long count;
        public final List<Pair<String, String>> dataPairs;
        public final String id;

        public SearchGetValuesResult(long count, List<Pair<String, String>> dataPairs, String id) {
            this.count = count;
            this.dataPairs = dataPairs;
            this.id = id;
        }
    }

    public static final class ShardResults {
        public final long count;
        public final List<Pair<String, String>> data;
        public final List<String> ids;

        public ShardResults(long count, List<Pair<String, String>> data, List<String> ids) {
            this.count = count;
            this.data = data;
            this.ids = ids;
        }
    }

    public static final class SearchShardsResult {
        public final long count;
        public final List<String> results;
        public final List<String> ids;

        public SearchShardsResult(long count, List<String> results, List<String> ids) {
            this.count = count;
            this.results = results;
            this.ids = ids;
        }
    }

    public static final class SearchZsetValuesResult {
        public final long count;
        public final Set<Tuple> data;
        public final String id;

        public SearchZsetValuesResult(long count, Set<Tuple> data, String id) {
            this.count = count;
            this.data = data;
            this.id = id;
        }
    }

    public static final class SearchShardsZsetResult {
        public final long count;
        public final List<String> results;
        public final List<String> ids;

        public SearchShardsZsetResult(long count, List<String> results, List<String> ids) {
            this.count = count;
            this.results = results;
            this.ids = ids;
        }
    }
}
