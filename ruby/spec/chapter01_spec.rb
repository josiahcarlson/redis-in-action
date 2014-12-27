require 'spec_helper'
require './chapter01'

describe 'chapter01' do
  let(:client) { Redis.new }

  describe '#article_vote' do
    it { expect(article_vote(client)).to be_nil }
  end
end
