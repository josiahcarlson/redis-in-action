ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE          = 432
ARTICLES_PER_PAGE   = 25

def article_vote(client, user, article)
  cutoff = Time.now.to_i - ONE_WEEK_IN_SECONDS
  return if client.zscore('time:', article) < cutoff

  article_id = article.split(':')[-1]

  if client.sadd("voted:#{article_id}", user)
    client.zincrby('score:', VOTE_SCORE, article)
    client.hincrby(article, 'votes', 1)
  end
end

def post_article(client, user, title, link)
  article_id = client.incr('article:')

  voted = "voted:#{article_id}"
  client.sadd(voted, user)
  client.expire(voted, ONE_WEEK_IN_SECONDS)

  now = Time.now.to_i
  article = "article:#{article_id}"

  client.mapped_hmset(
    article,
    {
      title:  title,
      link:   link,
      poster: user,
      time:   now,
      votes:  1,
    }
  )

  client.zadd('score:', now + VOTE_SCORE, article)
  client.zadd('time:', now, article)

  article_id
end

def get_articles(client, page, order = 'score:')
  start = (page - 1) * ARTICLES_PER_PAGE
  stop  = start + ARTICLES_PER_PAGE - 1
  ids   = client.zrevrange(order, start, stop)

  ids.inject([]) { |articles, id|
    articles << client.hgetall(id).merge(id: id)
  }
end

def add_remove_groups(client, article_id, to_add = [], to_remove = [])
  article = "article:#{article_id}"

  to_add.each do |group|
    client.sadd("group:#{group}", article)
  end

  to_remove.each do |group|
    client.srem("group:#{group}", article)
  end
end

def get_group_articles(client, group, page, order = 'score:')
  key = order + group

  unless client.exists(key)
    client.zinterstore(key, ["group:#{group}", order], aggregate: 'max')
    client.expire(key, 60)
  end

  get_articles(client, page, key)
end
