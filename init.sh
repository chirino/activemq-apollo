echo ================================================================
echo "Adding git remote: apache"
echo ================================================================
git remote add apache git://git.apache.org/activemq-apollo.git
git fetch apache

echo ================================================================
echo "Updating .git/authors.txt"
echo ================================================================
cd ".git"
curl "http://git.apache.org/authors.txt" > authors.txt
cd ".."
git config svn.authorsfile ".git/authors.txt"

echo ================================================================
echo "Initializing svn init"
echo ================================================================
git svn init --prefix="$REMOTE/" --tags=tags --trunk=trunk --branches=branches https://svn.apache.org/repos/asf/activemq/activemq-apollo
