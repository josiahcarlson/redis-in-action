ONE_WEEK_IN_SECONDS = 7 * 86400

def article_vote(client)
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
end
