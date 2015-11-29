**Overview**

  1. Maxtable consists of three components:
    * **Metadata server**: This provides the global namespace for all the tables in this system. It keeps the B-tree structure in memory.
    * **Ranger server**: It holds some ranges of the data and the default maximum range size is 1000MB.
    * **Client library**: The client library is linked with applications. This enables applications to read/write files stored in Maxtable.
  1. What components in the system and how they relate to one another. ![http://m3.img.libdd.com/farm2/164/A150C68C190405F366A822E6085632A4_461_247.jpg](http://m3.img.libdd.com/farm2/164/A150C68C190405F366A822E6085632A4_461_247.jpg)
  1. How to store the table in the disk ?
![http://m3.img.libdd.com/farm2/46/BF36B38EE226BA9ACAEB313BCC773D2E_540_143.jpg](http://m3.img.libdd.com/farm2/46/BF36B38EE226BA9ACAEB313BCC773D2E_540_143.jpg)
    * One SSTable = 4 M data.
    * One Tablet = 25K SSTable.
    * One Table = 42K Tablet.
So, one table can contain more than 4PB data, and we can extend the size of block or use two tablet levels to save index data to contain more data.