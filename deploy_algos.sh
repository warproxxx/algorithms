pause(){
 read -n1 -rsp $'Press any key to continue or Ctrl+C to exit...\n'
}

sudo apt-get update
sudo timedatectl set-timezone UTC
sudo apt-get install -y git

sudo rm ~/.ssh/id_rsa
sudo rm ~/.ssh/id_rsa.pub
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -C "dan13@tutanota.com" -q -N ""
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa
echo "Copy this key to gitlab:"
cat ~/.ssh/id_rsa.pub
pause

sudo apt install -y nginx python3 python3-dev python3-pip python3-setuptools build-essential nginx libffi-dev libssl-dev screen redis-server
ssh-keyscan gitlab.com >> $HOME/.ssh/known_hosts
ssh-keyscan github.com >> $HOME/.ssh/known_hosts
git clone git@gitlab.com:warproxxx/algorithms.git
python3 -m pip install -r algorithms/requirements.txt

sudo rm /etc/nginx/sites-enabled/default

if [ ! -f /etc/nginx/sites-available/node ]; then
    sudo touch /etc/nginx/sites-available/node
    echo 'server {' | sudo tee -a  /etc/nginx/sites-available/node
    echo '    listen 80;' | sudo tee -a  /etc/nginx/sites-available/node
    echo '    server_name algos.scfund.io;' | sudo tee -a  /etc/nginx/sites-available/node
    echo '' | sudo tee -a  /etc/nginx/sites-available/node
    echo '    location / {' | sudo tee -a  /etc/nginx/sites-available/node
    echo '        proxy_set_header   X-Forwarded-For $remote_addr;' | sudo tee -a  /etc/nginx/sites-available/node
    echo '        proxy_set_header   Host $http_host;' | sudo tee -a  /etc/nginx/sites-available/node
    echo '        proxy_pass         "http://127.0.0.1:8000";' | sudo tee -a /etc/nginx/sites-available/node
    echo '    }' | sudo tee -a /etc/nginx/sites-available/node
    echo '}' | sudo tee -a /etc/nginx/sites-available/node

    sudo ln -s /etc/nginx/sites-available/node /etc/nginx/sites-enabled/node
    sudo service nginx restart
fi

echo -e "\n"
curl checkip.amazonaws.com
read -p "Now modify the DNS record to this IP and press any key to continue"
pause

sudo service nginx stop
sudo apt-get install certbot python3-certbot-nginx -y
sudo certbot --nginx --preferred-challenges http -d algos.scfund.io --redirect --email daniel@scfund.io --agree-tos --no-eff-email

echo -e "\n\nNow add the API keys and start the program :)"