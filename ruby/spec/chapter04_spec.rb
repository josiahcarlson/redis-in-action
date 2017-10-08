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

  it "initalizes DB" do
    c = Chapter4::Client.new(conn, [user0, user1])

    expect(c.user(0) == {"name"=>"Jon", "fund"=>"100"}).to be_truthy
    expect(c.user(1) == {"name"=>"Arya", "fund"=>"300"}).to be_truthy

    expect(c.inventory(0) == ["Longclaw", "Ghost"]).to be_truthy
    expect(c.inventory(1) == ["Needle", "Frey Pies"]).to be_truthy
  end
end
