# Monitor-BTC-Wallet-Balances
Script to monitor a list of BTC wallet balances in near real time using Electrumx server. 

This script is used to monitor wallet addresses, the addresses must be converted to scripthash. 
I've included examples of an input csv of wallet addresses with their scripthash and the output csv of monitored balances.

I made this little project as a way to get BTC wallet activity (without paying for an API) from influential wallets, exchanges etc, with the end goal of creating a machine learning project where I combine this wallet activity with BTC price activity (historical aswell) to create buy/sell alerts. 
Using an overfitted swing trading strategy to train a model.. I don't expect to create a god level trading bot, but hoping to undertand more about machine learning and Tensorflow, later on..

This is very early days, I don't know if my approach will work very well, as electrumx server might not be fast enough registering the wallet activity... I will add a timestamp to record the delay from txn time to updated csv time.

