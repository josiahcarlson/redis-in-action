require 'spec_helper'
require './chapter01'

describe 'chapter01' do
  let(:client) { Redis.new }

  before do
    client.flushall
  end

  describe '#article_vote' do
    it { expect(article_vote(client)).to be_nil }
  end

  describe '#post_article' do
    let(:user) { 'username' }
    let(:title) { 'A title' }
    let(:link) { 'http://www.google.com' }

    it 'returns new article id' do
      expect(post_article(client, user, title, link)).to eq 1
    end

    it 'creates article with 5 attributes' do
      expect {
        post_article(client, user, title, link)
      }.to change { client.hlen('article:1') }.by(5)
    end

    it 'adds ZSET having score key' do
      post_article(client, user, title, link)
      expect(client.zscore('score:', 'article:1').to_s).to match /\d+\.0/
    end

    it 'adds ZSET having time key' do
      post_article(client, user, title, link)
      expect(client.zscore('time:', 'article:1').to_s).to match /\d+\.0/
    end
  end
end
