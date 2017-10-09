require 'spec_helper'
require './chapter04'

describe 'Chapter 4' do
  let(:conn) { Redis.new(db: 15) }

  let(:user0) {
    {
      id: 0,
      name: "Jon",
      fund: 100,
      items: ["Longclaw", "Ghost"]
    }
  }

  let(:user1) {
    {
      id: 1,
      name: "Arya",
      fund: 300,
      items: ["Needle", "Frey Pies"]
    }
  }

  before do
    conn.flushdb
  end

  it "inializes DB" do
    c = Chapter4::Client.new(conn, [user0, user1])

    expect(c.user(0)).to eq({"name"=>"Jon", "fund"=>"100"})
    expect(c.user(1)).to eq({"name"=>"Arya", "fund"=>"300"})

    expect(c.inventory(0)).to match_array(["Longclaw", "Ghost"])
    expect(c.inventory(1)).to match_array(["Needle", "Frey Pies"])
  end

  describe "list_item" do
    it "works" do
      c = Chapter4::Client.new(conn, [user0, user1])
      expect(c.market == []).to be_truthy

      c.list_item("Needle", 1, 50)
      expect(c.inventory(1)).to match_array(["Frey Pies"])
      expect(c.market).to eq([
        ["Needle.1", 50.0]
      ])

      c.list_item("Ghost", 0, 150)
      expect(c.inventory(0)).to match_array(["Longclaw"])
      expect(c.market).to eq([
        ["Needle.1", 50],
        ["Ghost.0", 150]
      ])

      c.list_item("Longclaw", 0, 90)
      expect(c.inventory(0)).to match_array([])
      expect(c.market).to eq([
        ["Needle.1", 50],
        ["Longclaw.0", 90],
        ["Ghost.0", 150]
      ])
    end
  end

  describe "purchase_item" do
    it "works" do
      c = Chapter4::Client.new(conn, [user0, user1])
      c.list_item("Ghost", 0, 150)
      expect(c.market).to eq([["Ghost.0", 150]])
      expect(c.user(0)["fund"].to_i).to eq(100)
      expect(c.user(1)["fund"].to_i).to eq(300)
      expect(c.inventory(1)).to match_array(["Needle", "Frey Pies"])

      c.purchase_item("Ghost", 0, 1)
      expect(c.market).to eq([])
      expect(c.user(0)["fund"].to_i).to eq(250)
      expect(c.user(1)["fund"].to_i).to eq(150)
      expect(c.inventory(1)).to match_array(["Ghost", "Needle", "Frey Pies"])
    end

    it "handles race condition with optimistic locking" do
      c = Chapter4::Client.new(conn, [user0, user1])
      c.list_item("Ghost", 0, 150)
      c.list_item("Longclaw", 0, 300)
      threads = ["Ghost", "Longclaw"].map do |item|
        # Thread.new { c.purchase_item_naive(item, 0, 1) }
        Thread.new { c.purchase_item(item, 0, 1) }
      end
      threads.each(&:join)

      jon_fund = c.user(0)["fund"].to_i
      arya_fund = c.user(1)["fund"].to_i

      # Arya's fund cannot be negative
      expect(arya_fund).to be >= 0

      # Arya has only the money to purchase ONE of the two items
      ghost_purchased = jon_fund == 250 && arya_fund == 150
      longclaw_purchased = jon_fund == 400 && arya_fund == 0
      expect(ghost_purchased || longclaw_purchased).to be_truthy
    end
  end
end
