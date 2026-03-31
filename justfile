# Run the full CI suite (lint + tests)
test: lint _test

lint:
    bundle exec rubocop

_test:
    bundle exec rake test

ci: test

bundle-update *ARGS:
    bundle update {{ARGS}}
