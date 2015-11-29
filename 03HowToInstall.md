**HOW TO CONFIGURE**<br>
If using the Kosmos file system as your back-end storage engine, you should set the MT_BACKEND with 'KFS'<br>
<pre><code>	export MT_BACKEND=KFS<br>
</code></pre>
and set the MT_DFS_CLI_LIB_PATH and MT_DFS_INCLUDE_PATH with your path of KFS's LIB and include.<br>
<pre><code>	export MT_DFS_CLI_LIB_PATH=~/kosmos/build/src/cc<br>
	export MT_DFS_INCLUDE_PATH=~/kosmos/src/cc<br>
</code></pre>
else if using the local file system as the storage engine, please set it as follows:<br>
<pre><code>	export MT_BACKEND=LOCAL<br>
</code></pre>
<b>HOW TO BUILD</b><br>
<ul><li>Checkout the source code<br>
<pre><code>svn checkout https://maxtable.googlecode.com/svn/trunk/ maxtable<br>
</code></pre>
</li><li>Enter into your_source_dir<br>
<pre><code>cd maxtable<br>
</code></pre>
</li><li>Build the source code<br>
<pre><code>make clean<br>
make<br>
</code></pre></li></ul>

<b>HOW TO RUN</b><br>
<ul><li>Enter into your_source_dir<br>
<pre><code>cd maxtable<br>
</code></pre>
</li><li>Edit the configure file<br>
<pre><code>vim cli.conf<br>
vim master.conf<br>
vim ranger.conf<br>
</code></pre>
</li><li>Boot KFS if you use the KFS as the back-end storage engine</li></ul>

<ul><li>Boot Master<br>
<pre><code>./starMaster<br>
</code></pre>
</li><li>Boot Ranger<br>
<pre><code>./starRanger<br>
</code></pre></li></ul>

<b>RUN SAMPLE</b>
<ul><li>Start the test in the client<br>
<pre><code>./sample create<br>
./sample insert<br>
./sample select<br>
./sample selectrange<br>
./sample selectwhere<br>
./sample selectcount<br>
./sample selectsum<br>
./sample crtindex<br>
./sample dropindex<br>
./sample delete<br>
./sample drop<br>
</code></pre></li></ul>

<b>RUN REGRESSION TESTS</b><br>