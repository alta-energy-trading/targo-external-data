$app_path = "C:\Dev\Projects\TargoExternalData\ArcadiaCoreImporter\CoreImporter\bin\Release\netcoreapp2.2\publish"
$settings_path = "C:\Dev\Projects\TargoExternalData\ArcadiaCoreImporter\CoreImporter"

$target_path = "\\arcsql\Targo\dlls\"
$target_environment = "Test"
$target_folder = "ArcadiaCoreImporter"
$appsettings_file = "appsetings.json"


Write-Host "Publish to what environment (Test/Live)? "
$environment_name = Read-Host
if ($environment_name.ToLower() -eq 'live') {
    $target_environment = ""
    $appsettings_file = "appsettings.prod.json"
} else {
    $target_environment = "$($environment_name )$('\')"
    $appsettings_file = "appsettings.test.json"
}

$target_fullPath = "$($target_path)$($target_environment)$($target_folder)"

Write-Host "Publishing to " $target_fullPath
get-childitem -path $app_path | copy-item -Destination $target_fullPath  -recurse -Force
get-childitem -path $settings_path -Filter $appsettings_file | copy-item -Destination "$($target_fullPath)$("\")$("appsettings.json")" -Force 
Write-Host "Publish Complete"

Read-Host
