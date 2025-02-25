Compile application
---------------------------------------------
Linux:      cd $GOPATH/src/bitbucket.org/modima/dbsync2/ && GOOS=linux GOARCH=amd64 go build -o dbsync2
Windows:    cd $GOPATH/src/bitbucket.org/modima/dbsync2/ && GOOS=windows GOARCH=amd64 go build -o dbsync2.exe
Macintosh:  cd $GOPATH/src/bitbucket.org/modima/dbsync2/ && GOOS=darwin GOARCH=amd64 go build -o dbsync2_mac

Build Package
---------------------------------------------
0. Alle Änderungen commiten

1. Build Verzeichnis löschen

    $ rm -rf build

2. Tag für aktuelle Version erstellen

    $ git tag 1.0.1 && git push origin 1.0.1

3. Build-Verzeichnis anlegen

    $ mkdir build && cd build

4. Aktuelle Version aus Repository laden

    $ dh-make-golang bitbucket.org/modima/dbsync2 

5. Debian Verzeichnis kopieren

    $ cp -r ../debian/ dbsync2/

6. Changelog aktualisieren (Version muss orig.tar.gz entsprechen)

    $ nano dbsync2/debian/changelog

7. Changelog zurückkopieren

    $ cp -r dbsync2/debian/ ../

8. Debian SOURCE Paket bauen (Option -S unbedingt verwenden, sonst wird es später von launchpad nicht akzeptiert)

    $ cd dbsync2 && debuild -S

9. Paket ins PPA laden

    $ dput -f ppa:cloud-it/ppa ../*.changes

10. Upload und Build Status prüfen auf https://launchpad.net/~cloud-it/+archive/ubuntu/ppa
