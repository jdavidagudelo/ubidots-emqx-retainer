FROM erlang:22
WORKDIR /home/ubidots/source/
COPY emqx-retainer .
RUN mv retainer_test_data/ retainer_changer/
CMD ["sleep", "infinity"]