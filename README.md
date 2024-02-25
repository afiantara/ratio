# ratio

database insurance.db merupakan gabungan dari finansial statement dan income untuk masing-masing produk asuransi: life dan non life.
path url : ./datas/insurance.db

fs.py adalah library yang diperlukan untuk membaca seluruh data hasil download dari publikasi OJK.
app.py adalah aplikasi utama yang digunakan untuk memproses data dari publikasi OJK menjadi hasil olahan data seperti:
  - menghitung delta
  - menghitung moving average 12 bulan
  - menghitung rasio (ROE)
