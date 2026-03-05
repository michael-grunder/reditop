## General instructions

* Prefer modern PHP syntax and idioms. We won't run this on anything < 8.4.
* Prefer modularity and generic code over duplication unless there is a good
  performance reason for it.
* Prefer defensive programming. Never ignore return values when they can fail
  unless there is a compelling reason (rarely we may not care about a failure).
* Prefer using idiomatic composer packages where appropriate over hand rolled solutions.
  There can be exceptions such as the package bing much heavier weight than needed or
  if there are specific performance or integration conflcts.
* When adding features never just tack them on to existing code and pepper
  specific handling code all around the codebase for this new feature. Instead
  we always want to maingain a clean architecture, so if the feature requires
  a sizeable redesign to do so, always prefer that.
* Wrappiing `phpredis` in a helper class is fine, but don't dispatch via
  `__call` especially for any method that takes a reference (e.g. `scan`).
* After modifying source code make sure they compile. e.g quick-lint-js for
  JS/TS, pylint for Python, etc, php -l for PHP.
* Run vendor/bin/phpstan analyze and fix any reported issues if phpstan is used in the
  project.
* Run vendor/bin/phpunit to make sure tests pass if we're using phpunit.
* Remember to update `README.md` if the changes change what is documented.
* After each change create or add `CHANGELOG.md`. As changes are added they go
  under `## Unreleased` and then at time of tag will be formalized. Within each
  version group changes into sections like `### Fixed`, `### Added`,
  `### Changed`, `### Deprecated`, `### Removed`.
