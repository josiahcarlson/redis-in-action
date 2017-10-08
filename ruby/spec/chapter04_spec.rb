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

    expect(c.user(0)).to eq({"name"=>"Jon", "fund"=>"100"})
    expect(c.user(1)).to eq({"name"=>"Arya", "fund"=>"300"})

    expect(c.inventory(0)).to eq(["Longclaw", "Ghost"])
    expect(c.inventory(1)).to eq(["Needle", "Frey Pies"])
  end

  # Section 4.4.2
  it "lists item" do
    c = Chapter4::Client.new(conn, [user0, user1])
    expect(c.market == []).to be_truthy

    c.list_item("Needle", 1, 50)
    expect(c.inventory(1)).to eq(["Frey Pies"])
    expect(c.market).to eq([
      ["Needle.1", 50.0]
    ])

    c.list_item("Ghost", 0, 150)
    expect(c.inventory(0)).to eq(["Longclaw"])
    expect(c.market).to eq([
      ["Needle.1", 50],
      ["Ghost.0", 150]
    ])

    c.list_item("Longclaw", 0, 90)
    expect(c.inventory(0)).to eq([])
    expect(c.market).to eq([
      ["Needle.1", 50],
      ["Longclaw.0", 90],
      ["Ghost.0", 150]
    ])

    c.list_item("sdkljfsdkljfsdlk", 0, 1000)
    expect(c.market).to eq([
      ["Needle.1", 50],
      ["Longclaw.0", 90],
      ["Ghost.0", 150]
    ])
  end
end
