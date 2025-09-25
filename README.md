# Setup instructions

- Tested on Arch linux and OSX
- Arch linux
    - `yay -S python-uv`
- OSX
    - `brew install uv`
- `uv sync --frozen --all-packages`
- `docker compose up -d --build`
- `inv -e test.test-all`