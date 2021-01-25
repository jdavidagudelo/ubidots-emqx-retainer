Install erlang on ubuntu:

    sudo apt-get install erlang

Install rebar3:

    wget https://s3.amazonaws.com/rebar3/rebar3 && chmod +x rebar3
    sudo mv rebar3 /usr/bin/rebar3

To configure redis for testing add this to /etc/hosts:

    127.0.0.1 redis

To run code validations:

    make xref eunit ct cover


