# Mario

The Pipeline Runner

## Credits

Created for the [CLAMS project](https://clams.ai/) by GBH Media Library and Archives

## develop

### pre-commit secret scanning

0. Install [ggshield](https://docs.gitguardian.com/ggshield-docs/getting-started)

```shell
pip install ggshield
# or
brew install gitguardian/tap/ggshield
```

1. Login to gitguardian

```shell
ggshield auth login
```

2. Install the pre-commit hooks

```shell
pre-commit install
```
