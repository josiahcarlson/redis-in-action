require 'spec_helper'
require './chapter01'

describe 'chapter01' do
  let(:client) { Redis.new }

  before do
    client.flushdb
  end

  describe 'article' do
    it 'is available to be posted, voted, gotten, added remove groups' do
      article_id = post_article(client, 'username', 'A title', 'http://www.google.com')
      puts "We posted a new article with id: #{article_id}"
      expect(article_id).to be_truthy

      puts "Its HASH looks like:"
      article = client.hgetall("article:#{article_id}")
      puts article
      expect(hash).to be_truthy

      article_vote(client, 'other_user', "article:#{article_id}")
      puts "We voted for the article, it now has votes:"
      votes = client.hget("article:#{article_id}", 'votes').to_i
      puts votes
      expect(votes > 1).to be_truthy

      puts "The currently highest-scoring articles are:"
      articles = get_articles(client, 1)
      puts articles
      expect(articles.count >= 1).to be_truthy

      add_remove_groups(client, article_id, ['new-group'])
      puts "We added the article to a new group, other articles include:"
      articles = get_group_articles(client, 'new-group', 1)
      puts articles
      expect(articles.count >= 1).to be_truthy
    end
  end

  describe 'post_article' do
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
