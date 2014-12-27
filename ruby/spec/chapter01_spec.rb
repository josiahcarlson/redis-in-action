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

    it { expect(post_article(client, user, title, link)).to eq 1 }
  end
end
