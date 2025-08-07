# Install zip
sudo apt-get update
sudo apt-get install -y curl
sudo apt-get install -y zip

# Install Oh My Posh
curl -s https://ohmyposh.dev/install.sh | bash -s -- -t "/.local/bin/themes/"

# Add /bin to the PATH environment variable
$env:PATH += ":/bin"

sudo apt-get install -y fontconfig
oh-my-posh font install CascadiaCode 

# Suggest setting font to Cascadia Code NFM in vs code settings
# "terminal.integrated.fontFamily": "CaskaydiaCove Nerd Font, monospace",
Install-Module -Name Terminal-Icons -Scope CurrentUser -Force
Install-Module -Name posh-git -Scope CurrentUser -Force

# Install Git 
sudo apt install git -y

#### GH CLI for Authentication with GitHub ####
bash ./scripts/git_cli_install.sh 

# Copy sample profile to home directory
if (-not (Test-Path -Path $PROFILE)) {
    New-Item -ItemType File -Path $PROFILE -Force
}
cp ./scripts/pwsh_profile_sample.txt $PROFILE

# Pip Installs 
pip install uv
uv venv
./.venv/bin/activate.ps1
pip install uv
uv sync

# NPM
bash ./scripts/npm_install.sh