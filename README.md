# db-android
An observable key/value database built on top of SQLCipher (SQLite)

Uses modified version of https://github.com/thegedge/java-radix-tree.

## ktlint
This project is formatted and linted with ktlint using the [ktlint-gradle plugin](https://github.com/JLLeitschuh/ktlint-gradle).

You can install the [ktlint Intellij plugin](https://plugins.jetbrains.com/plugin/15057-ktlint-unofficial-)
for some support for linting within Android Studio.

### Add Commit Hook
./gradlew addKtlintCheckGitPreCommitHook

This adds a pre commit hook that lints all staged files upon commit.

### Manually Auto-format
./gradlew ktlintFormat

This auto-formats all Kotlin files in the project.

### Manually Check
./gradlew ktlintCheck

This manually runs the linter against all Kotlin files in the project.