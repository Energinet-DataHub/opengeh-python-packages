ARG UBUNTU_VERSION=24.04

# Base image
FROM mcr.microsoft.com/devcontainers/base:ubuntu-${UBUNTU_VERSION}

ARG ASDF_VERSION=0.16.0

# Configure user
ENV USER=vscode
ENV HOME=/home/$USER

# Install System Dependencies
RUN apt-get update \
  && apt-get install -y \
  git \
  curl \
  build-essential \
  # gnupg2 allows you to sign git commits with GPG
  gnupg2 \
  gcc \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get clean

# Add tool-versions and startup script
COPY --chown=${USER}:${USER} scripts $HOME/scripts
RUN chmod +x $HOME/scripts/*

# Install asdf
RUN $HOME/scripts/install-asdf.sh $ASDF_VERSION \
  && echo 'export PATH="${ASDF_DATA_DIR:-$HOME/.asdf}/shims:$PATH"' >> $HOME/.bashrc \
  && echo 'export PATH="${ASDF_DATA_DIR:-$HOME/.asdf}/shims:$PATH"' >> $HOME/.zshrc

# Install Databricks CLI
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Persist command history
RUN mkdir /commandhistory \
  && touch /commandhistory/.bash_history \
  && touch /commandhistory/.zsh_history \
  && chown -R $USER /commandhistory \
  && sed -i "1iexport PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" $HOME/.bashrc \
  && sed -i "1iexport PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.zsh_history" $HOME/.zshrc

# Install zsh-autosuggestions
RUN git clone https://github.com/zsh-users/zsh-autosuggestions ~/.zsh/zsh-autosuggestions \
  && echo "source ~/.zsh/zsh-autosuggestions/zsh-autosuggestions.zsh" >> $HOME/.zshrc
