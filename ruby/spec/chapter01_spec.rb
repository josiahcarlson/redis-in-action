require 'spec_helper'
require './chapter01'

describe 'Chapter 1' do
  let(:client) { Redis.new(db: 15) }

  before do
    client.flushdb
  end

  it "tests article functionality" do
    article_id = post_article(client, 'username', 'A title', 'http://www.google.com')
    puts 'We posted a new article with id:', article_id
    expect(article_id).to be_truthy

    puts 'Its HASH looks like:'
    article = client.hgetall("article:#{article_id}")
    puts article
    expect(article).to be_truthy

    article_vote(client, 'other_user', "article:#{article_id}")
    puts 'We voted for the article, it now has votes:'
    votes = client.hget("article:#{article_id}", 'votes').to_i
    puts votes
    expect(votes > 1).to be_truthy

    puts 'The currently highest-scoring articles are:'
    articles = get_articles(client, 1)
    puts articles
    expect(articles.count >= 1).to be_truthy

    add_remove_groups(client, article_id, ['new-group'])
    puts 'We added the article to a new group, other articles include:'
    articles = get_group_articles(client, 'new-group', 1)
    puts articles
    expect(articles.count >= 1).to be_truthy
  end
end
