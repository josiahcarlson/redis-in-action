def article_vote(client)
end

def post_article(client, user, title, link)
  article_id = client.incr('article:')
end
