# Overview

This simple facade is the boundary of the rocket status context.

## Usage

After cloning, install packages

```sh
$ npm i
```

build

```sh
$ ./node_modules/.bin/tsc
```

deploy

```sh
$ npx @riddance/deploy dev1
```

where `dev1` is the namespace (aka. prefix). Deployment requires valid AWS credentials stored in `~/.aws/credentials`.
