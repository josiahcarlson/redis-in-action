module Chapter4
  class Client
    def initialize(conn, users)
      @conn = conn
      init_db(users)
    end

    # =============== Section 4.4.2 ================== #
    def list_item(item, seller_id, price)
      u = User.new(seller_id)
      @conn.multi do |multi|
        multi.srem(u.inventory_id, item)
        multi.zadd("market", price, u.item_id(item))
      end
    end

    # =============== Section 4.4.3 ================== #
    def purchase_item(item, seller_id, buyer_id)
      seller = User.new(seller_id)
      buyer = User.new(buyer_id)
      @conn.watch(["market", buyer.id]) do
        fund = @conn.hget(buyer.id, "fund")
        price = @conn.zscore("market", seller.item_id(item))

        if price && price <= fund.to_i
          @conn.multi do |multi|
            multi.hincrby(buyer.id, "fund", -price.to_i)
            multi.hincrby(seller.id, "fund", price.to_i)
            multi.zrem("market", seller.item_id(item))
            multi.sadd(buyer.inventory_id, item)
          end
        else
          @conn.unwatch
        end
      end
    end

    # Without locking
    def purchase_item_naive(item, seller_id, buyer_id)
      seller = User.new(seller_id)
      buyer = User.new(buyer_id)
      fund = @conn.hget(buyer.id, "fund")
      price = @conn.zscore("market", seller.item_id(item))

      return if !price || price > fund.to_i

      @conn.multi do |multi|
        multi.hincrby(buyer.id, "fund", -price.to_i)
        multi.hincrby(seller.id, "fund", price.to_i)
        multi.zrem("market", seller.item_id(item))
        multi.sadd(buyer.inventory_id, item)
      end
    end

    # ================================================ #

    def user(id)
      u = User.new(id)
      @conn.hgetall(u.id)
    end

    def inventory(id)
      u = User.new(id)
      @conn.smembers u.inventory_id
    end

    def market
      @conn.zrange("market", 0, -1, with_scores: true)
    end

    private

    def init_db(users)
      @conn.pipelined do
        users.each { |u| init_user(u) }
      end
    end

    def init_user(id:, name:, fund:, items:)
      u = User.new(id)
      @conn.hmset(u.id, "name", name, "fund", fund)
      items.each { |item| @conn.sadd(u.inventory_id, item) }
    end
  end

  class User
    def initialize(id)
      @id = id
    end

    def id
      "user:#{@id}"
    end

    def inventory_id
      "inventory:#{@id}"
    end

    def item_id(item)
      "#{item}.#{@id}"
    end
  end
end
