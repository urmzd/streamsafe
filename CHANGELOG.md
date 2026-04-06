# Changelog

## 0.2.0 (2026-04-06)

### Features

- **examples**: add fight-flow and finance-flow pipeline examples ([81fdbfe](https://github.com/urmzd/streamsafe/commit/81fdbfe2d9cce0c522169e8dbda3aca8bb69fe0a))

### Bug Fixes

- **examples**: resolve clippy and fmt errors in fight-flow ([0300504](https://github.com/urmzd/streamsafe/commit/0300504704584113a5a5171ec0f47252e2bbd3d4))

[Full Changelog](https://github.com/urmzd/streamsafe/compare/v0.1.0...v0.2.0)


## 0.1.0 (2026-04-06)

### Features

- **media**: implement ffmpeg file decoder and audio filter transform ([1429e19](https://github.com/urmzd/streamsafe/commit/1429e192b418c2d1ff07fb428d2ecc1d4497b30e))
- **core**: add broadcast sink and filter transform primitives ([be24530](https://github.com/urmzd/streamsafe/commit/be24530a1c6dd9444c569930c5a7892c6cef2166))
- **deps**: add ffmpeg support with binding and feature flag ([845225a](https://github.com/urmzd/streamsafe/commit/845225ac9405be33aed7a3881dd5c5aa08f4b24b))
- **media**: add media feature with frame model and nodes ([435bfe0](https://github.com/urmzd/streamsafe/commit/435bfe0d09a100316b597d86def2639a119b053e))
- **core**: implement pipeline framework traits and builder ([7082bd6](https://github.com/urmzd/streamsafe/commit/7082bd6b9afcd4a0b1e1d9111988aa341dbb1232))
- add redpanda and minio ([d4781e7](https://github.com/urmzd/streamsafe/commit/d4781e7c6f60c9ea318ab747e7c94ad5d801ae7c))
- add multifilesink message retrievals ([c6d259e](https://github.com/urmzd/streamsafe/commit/c6d259e4080961b31b0f28d6d0463ed16f938e98))
- safe exit ([fc0d87f](https://github.com/urmzd/streamsafe/commit/fc0d87f907987cec8bf7d59fc4db00c9744e41d9))
- add env var support ([000e91a](https://github.com/urmzd/streamsafe/commit/000e91a2883e974d55477b4956a29294aeb2e0b4))
- run rtsp server ([75b19ac](https://github.com/urmzd/streamsafe/commit/75b19acbf72069161e9177ad0ffcee3d9e9a7cfa))

### Bug Fixes

- **ci**: upgrade sr action from v2 to v3 ([37d2fde](https://github.com/urmzd/streamsafe/commit/37d2fde1df22f5cda350a4022359d74d33b2ff11))
- add ability to split at desired frames ([ab4adf8](https://github.com/urmzd/streamsafe/commit/ab4adf8d2dd4eeb4a4d90bbe4276c8bc943cda83))
- get splitmux running ([2bbedfe](https://github.com/urmzd/streamsafe/commit/2bbedfed627593473b9600d429cf630f272d523b))
- get a running mp4 ([93244b8](https://github.com/urmzd/streamsafe/commit/93244b86b2aafe95bce13749b29cc6169d645eef))
- ensure cleanup ([8edf080](https://github.com/urmzd/streamsafe/commit/8edf080b6a59de92b96568c611204c630c9369ca))
- remove video source ([1adec9b](https://github.com/urmzd/streamsafe/commit/1adec9b92ed6af337556d3d9b9c3b4b7239bfcfa))
- update to use png ([c380c07](https://github.com/urmzd/streamsafe/commit/c380c07aae2cd4495cbbe3d217c6f1495126f3c8))
- lsp ([933b1ac](https://github.com/urmzd/streamsafe/commit/933b1ac85195227bd9ad33621358e0ca17d14139))

### Documentation

- **examples**: add rtsp and audio extraction examples ([2a35920](https://github.com/urmzd/streamsafe/commit/2a359203c8f9561e4e1fd5408844d6a2563b38f3))
- add comprehensive readme with examples and architecture ([75ef13d](https://github.com/urmzd/streamsafe/commit/75ef13dab1aeee61734306ebffb80ee850d20e34))
- add agent skill following agentskills.io spec ([9b51414](https://github.com/urmzd/streamsafe/commit/9b51414e310a32a0864c72682502d5bf342cc817))

### Refactoring

- remove legacy monolithic binary ([54ec996](https://github.com/urmzd/streamsafe/commit/54ec9968c678bc2dde48d89ab3fa1cb988dd6c6b))
- links ([7ac6dc3](https://github.com/urmzd/streamsafe/commit/7ac6dc349832fb3b0334f13667cc651a3e872ee2))

### Miscellaneous

- reformat code for consistency ([f5477c5](https://github.com/urmzd/streamsafe/commit/f5477c5df9e0e8f41b4d7fa91f6fdca7ca0e335b))
- **infra**: remove .envrc flake reference ([946ae56](https://github.com/urmzd/streamsafe/commit/946ae560cd450675661ce3b47b31b8062d6db7bb))
- **deps**: update cargo.lock with ffmpeg support ([2770416](https://github.com/urmzd/streamsafe/commit/277041653b0c40dc6f6296b41b75a1809c0c8f79))
- **workflows**: add github actions for testing and release ([d11ea60](https://github.com/urmzd/streamsafe/commit/d11ea601efa3a03873c43119bcf326283aaa289d))
- **justfile**: add common build and development tasks ([db2f9cb](https://github.com/urmzd/streamsafe/commit/db2f9cb2693914eb0d40ce1b1cecc872905beaf0))
- **infra**: setup direnv and nix development environment ([d1361ff](https://github.com/urmzd/streamsafe/commit/d1361ff64a777c9757ea95c6d2cbf531e9708efc))
- **deps**: update cargo.lock with media dependencies ([35977d2](https://github.com/urmzd/streamsafe/commit/35977d2e203b4a61180f533889f8723488e10958))
- **workspace**: initialize cargo workspace structure ([2aa855c](https://github.com/urmzd/streamsafe/commit/2aa855cd6a7ccd44ec2d859fe95e8e789bea6e56))
- license under Apache 2.0 ([434079d](https://github.com/urmzd/streamsafe/commit/434079d9d182dcd6a447972fa180e0596864ab03))
