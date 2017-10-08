module Chapter4
  class Client
    def initialize(conn, users)
      @conn = conn
      init_db(users)
    end

    def market
      @conn.zrange("market", 0, -1, with_scores: true)
    end

    def user(id)
      @conn.hgetall("user:#{id}")
    end

    private

    def init_db(users)
      @conn.pipelined do
        users.each { |u| init_user(u) }
      end
    end

    def init_user(id:, name:, fund:, items:)
      @conn.hmset("user:#{id}", "name", name, "fund", fund)
      items.each { |item| @conn.sadd("inventory:#{id}", item) }
    end
  end
end
