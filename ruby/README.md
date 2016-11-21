Redis in Action (Ruby code samples)
===================================

### System Setup

**NOTE:** Tested on Mac OS X 10.8, Ruby 2.3.1, Redis 3.2.1

Install the latest stable version of Ruby (2.3.1) via rvm or rbenv

```
rvm install 2.3.1
rvm use 2.3.1
gem install bundler
```

Install the latest stable version of Redis via Homebrew

```
brew install redis
```

Clone the code base and install pre-requisite gems

```
git clone git@github.com:josiahcarlson/redis-in-action.git
cd redis-in-action
bundle install
```

---

To run test suite:

```
bundle exec rspec spec/chapter01_spec.rb
```
