## General instructions

* Prefer modularity and generic code over duplication unless there is a good
  performance reason for it.
* Prefer defensive programming. Never ignore return values when they can fail
  unless there is a compelling reason (rarely we may not care about a failure).
* Make sure to use Rust's best features where appropriate, for example traits
  and generics to abstract over different implementations of current and future
  functionality.
* Prefer using idiomatic cargo packages  rather than rolling our own unless
  there is a compelling reason.
* When adding features never just tack them on to existing code and pepper
  specific handling code all around the codebase for this new feature. Instead
  we always want to maingain a clean architecture, so if the feature requires
  a sizeable redesign to do so, always prefer that.
* Always run `cargo clippy` and follow the suggestions.
* After implementing new functionality remember to add tests where applicable.
* Always run `cargo test` before considerinig the task complete.
* Remember to update `README.md` if the changes change what is documented.
* After each change create or add `CHANGELOG.md`. As changes are added they go
  under `## Unreleased` and then at time of tag will be formalized. Within each
  version group changes into sections like `### Fixed`, `### Added`,
  `### Changed`, `### Deprecated`, `### Removed`.
