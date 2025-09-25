<?php
// db.config.php

$db_host = '193.203.175.227';
$db_user = 'u588900443_medicos';
$db_pass = 'Mastertelecom@2025';
$db_name = 'u588900443_medicos';

try {
    $pdo = new PDO("mysql:host=$db_host;dbname=$db_name;charset=utf8mb4", $db_user, $db_pass);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
} catch (PDOException $e) {
    die("Erro ao conectar ao banco de dados: " . $e->getMessage());
}
?>
