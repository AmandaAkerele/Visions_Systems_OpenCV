i would like to ensure that the file is created with the name of the dataframe and there shouldnt be a folder created when using spark in creating this cvc file

los_site_22.write.csv('delete_los_org_22.csv', header = True, mode='overwrite')
